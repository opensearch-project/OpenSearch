/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobVersionConflictException;
import org.opensearch.common.blobstore.VersionedBlob;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.Locale;

/**
 * Object-store-backed primary fencing for a shard. Maintains a small mutable per-shard {@code fence} blob in the
 * translog repository, updated exclusively via compare-and-swap ({@link BlobContainer#writeBlobConditionally}).
 * <p>
 * The fencing token is the CAS chain (the blob's opaque version token), not the primary term: every acknowledged
 * translog upload advances the chain via {@link #validateAndAdvance}, so any second writer — a stale primary after
 * failover, or a zombie relocation source — holds a stale token and fails its next CAS with a
 * {@link TranslogFencedException}, before the write is acknowledged. The CAS runs <b>concurrently</b> with the
 * immutable translog metadata upload to stay off the latency path; the upload is acknowledged only when both
 * succeed. A fenced writer may therefore publish one orphan metadata file — never acknowledged, term-scoped, and
 * ignored by readers that follow the highest-term lineage; cleaning such orphans up is a follow-up.
 * <p>
 * The fence blob is control-plane only: it is never read by snapshot restore, pinned-timestamp resolution or GC, and
 * all snapshot-referenced files remain immutable with unchanged names.
 * <p>
 * Note: bootstrap (and therefore the seal to a new primary term) currently happens lazily on the first upload of a
 * new primary. Sealing before the recovery restore-point read — required to fully close the acked-write-loss window
 * during failover — is a follow-up that integrates the seal into the remote store restore path.
 *
 * @opensearch.internal
 */
public class RemoteStoreFence {

    public static final String FENCE_BLOB_NAME = "fence";
    private static final String CODEC_VERSION = "v1";
    private static final String FIELD_SEPARATOR = "|";

    private final BlobContainer blobContainer;
    private final String ownerNodeId;
    private final ThreadPool threadPool;
    private final Logger logger;

    // Guarded by synchronized methods. Uploads on the ack path are effectively serialized by the translog sync
    // permits, but the fence must never issue two CAS calls with the same token.
    private String versionToken;
    private long term = -1;
    private long seq = -1;

    public RemoteStoreFence(BlobContainer blobContainer, String ownerNodeId, ShardId shardId, ThreadPool threadPool) {
        this.blobContainer = blobContainer;
        this.ownerNodeId = ownerNodeId;
        this.threadPool = threadPool;
        this.logger = Loggers.getLogger(getClass(), shardId);
    }

    /**
     * Asynchronous variant of {@link #validateAndAdvance} allowing the fence CAS to run concurrently with the
     * translog metadata upload so it stays off the acknowledgement latency path.
     */
    public void validateAndAdvanceAsync(long primaryTerm, ActionListener<Void> listener) {
        threadPool.executor(ThreadPool.Names.TRANSLOG_TRANSFER).execute(() -> {
            try {
                validateAndAdvance(primaryTerm);
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Validates that this shard copy still owns the fence and advances the CAS chain. Must be called on every
     * translog upload before the metadata file is published. The first call bootstraps the fence: it creates the
     * blob if absent, or seals it over to this owner if the supplied primary term is at least the fence's term.
     *
     * @param primaryTerm the primary term this upload is being performed at
     * @throws TranslogFencedException if another shard copy owns the fence — fatal, the caller must stop acking
     * @throws IOException on repository errors
     */
    public synchronized void validateAndAdvance(long primaryTerm) throws IOException {
        if (versionToken == null) {
            bootstrap(primaryTerm);
            return;
        }
        if (primaryTerm < term) {
            throw new TranslogFencedException(
                String.format(Locale.ROOT, "primary fenced by remote store: local term [%d] < fence term [%d]", primaryTerm, term)
            );
        }
        try {
            cas(versionToken, primaryTerm, seq + 1);
        } catch (BlobVersionConflictException e) {
            // We lost the CAS chain: another writer advanced the fence. Re-read to report who fenced us.
            throw fencedException(e);
        }
    }

    private void bootstrap(long primaryTerm) throws IOException {
        // One retry: a conflict during bootstrap can be a benign race (e.g. our own previous incarnation's write
        // landing), but a second conflict means another writer is actively advancing the fence.
        for (int attempt = 0; attempt < 2; attempt++) {
            VersionedBlob blob = null;
            try {
                blob = blobContainer.readBlobWithVersion(FENCE_BLOB_NAME);
            } catch (NoSuchFileException e) {
                // first ever fence for this shard, create-if-absent below
            }
            final String expectedToken;
            final long nextSeq;
            if (blob != null) {
                FenceState remote = FenceState.parse(blob.content());
                if (remote.term > primaryTerm) {
                    throw new TranslogFencedException(
                        String.format(
                            Locale.ROOT,
                            "primary fenced by remote store: local term [%d] < fence term [%d] owned by [%s]",
                            primaryTerm,
                            remote.term,
                            remote.owner
                        )
                    );
                }
                // remote.term == primaryTerm with a different owner is a legitimate takeover, not a conflict: primary
                // relocation hands the shard over at a constant term, so the target must be able to claim the chain.
                // Treating it as a fence here would make relocation impossible. Ordering between the two copies is
                // still enforced by the CAS: whoever claims the chain second wins, and the loser fails its next
                // upload. A relocation that is claimed and then aborted therefore fences the source, which is safe
                // (the shard fails and is reassigned) but coarse; wiring the handoff through an explicit ownership
                // CAS is a follow-up.
                expectedToken = blob.versionToken();
                nextSeq = remote.seq + 1;
            } else {
                expectedToken = null;
                nextSeq = 0;
            }
            try {
                cas(expectedToken, primaryTerm, nextSeq);
                logger.info("Fence bootstrapped: term [{}] owner [{}] seq [{}]", primaryTerm, ownerNodeId, nextSeq);
                return;
            } catch (BlobVersionConflictException e) {
                logger.info("Fence bootstrap CAS conflict on attempt [{}], retrying", attempt + 1);
            }
        }
        throw fencedException(null);
    }

    private void cas(String expectedToken, long primaryTerm, long nextSeq) throws IOException {
        byte[] content = new FenceState(primaryTerm, ownerNodeId, nextSeq).toBytes();
        String newToken = blobContainer.writeBlobConditionally(
            FENCE_BLOB_NAME,
            new ByteArrayInputStream(content),
            content.length,
            expectedToken
        );
        this.versionToken = newToken;
        this.term = primaryTerm;
        this.seq = nextSeq;
    }

    private TranslogFencedException fencedException(Exception cause) {
        String remoteDescription;
        try {
            FenceState remote = FenceState.parse(blobContainer.readBlobWithVersion(FENCE_BLOB_NAME).content());
            remoteDescription = String.format(Locale.ROOT, "term [%d] owner [%s] seq [%d]", remote.term, remote.owner, remote.seq);
        } catch (Exception e) {
            remoteDescription = "unreadable";
        }
        TranslogFencedException exception = new TranslogFencedException(
            String.format(
                Locale.ROOT,
                "primary fenced by remote store: fence owned by another writer [%s], local owner [%s] term [%d]",
                remoteDescription,
                ownerNodeId,
                term
            )
        );
        if (cause != null) {
            exception.addSuppressed(cause);
        }
        return exception;
    }

    // Visible for testing
    synchronized long getTerm() {
        return term;
    }

    // Visible for testing
    synchronized long getSeq() {
        return seq;
    }

    /**
     * Fence blob content: {@code v1|<term>|<ownerNodeId>|<seq>} in UTF-8.
     */
    static final class FenceState {
        final long term;
        final String owner;
        final long seq;

        FenceState(long term, String owner, long seq) {
            // Node ids are base64 UUIDs today, so this cannot fire; encoding an owner containing the separator would
            // silently produce a blob that parse() rejects, which would fence a healthy primary.
            if (owner.contains(FIELD_SEPARATOR)) {
                throw new IllegalArgumentException("Fence owner [" + owner + "] must not contain [" + FIELD_SEPARATOR + "]");
            }
            this.term = term;
            this.owner = owner;
            this.seq = seq;
        }

        byte[] toBytes() {
            return String.join(FIELD_SEPARATOR, CODEC_VERSION, Long.toString(term), owner, Long.toString(seq))
                .getBytes(StandardCharsets.UTF_8);
        }

        static FenceState parse(byte[] content) throws IOException {
            String[] tokens = new String(content, StandardCharsets.UTF_8).split("\\" + FIELD_SEPARATOR);
            if (tokens.length != 4 || CODEC_VERSION.equals(tokens[0]) == false) {
                throw new IOException("Unrecognized fence blob content");
            }
            try {
                return new FenceState(Long.parseLong(tokens[1]), tokens[2], Long.parseLong(tokens[3]));
            } catch (NumberFormatException e) {
                throw new IOException("Unrecognized fence blob content", e);
            }
        }
    }
}
