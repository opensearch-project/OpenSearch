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
import java.util.Objects;

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
 * A recovering primary claims the fence <b>before</b> it reads its translog restore point (see
 * {@code RemoteFsTranslog#sealFence}), which is what closes the acked-write-loss window during failover: a previous
 * primary that is still alive but no longer in the cluster's view has its token invalidated at that point, so it can
 * acknowledge nothing that lands after the restore point the new copy read. A copy receiving a primary relocation
 * handoff deliberately does not seal, since the source is still legitimately serving at the same term.
 * <p>
 * <b>Invariants.</b> Referenced as {@code I1}..{@code I8} from the code that enforces them:
 * <ol>
 * <li><b>I1 — the chain gates the ack.</b> An upload is acknowledged only if its fence CAS succeeded; the CAS is
 * joined before the acknowledgement in {@code TranslogTransferManager#transferSnapshot}.</li>
 * <li><b>I2 — one writer at a time.</b> Any successful fence write invalidates every other holder's token; enforced
 * by the repository's conditional-write semantics, which is why {@code RemoteFsTranslog#buildFence} refuses a
 * repository without them rather than running unfenced.</li>
 * <li><b>I3 — seal before restore.</b> Every writer transition except a same-term relocation handoff claims the
 * chain before reading the translog restore point it will serve from; asserted at the read choke point in
 * {@code IndexShard#syncRemoteTranslogAndUpdateGlobalCheckpoint}.</li>
 * <li><b>I4 — the term never regresses.</b> A claimant below the fence term is refused ({@link #validateAndAdvance},
 * {@link #bootstrap}); a same-or-higher term may always claim, since relocation and in-place snapshot restore are
 * legitimate same-term takeovers, and the chain — not the term — arbitrates between concurrent claimants.</li>
 * <li><b>I5 — seq strictly increases along the chain</b> (asserted in {@link #cas}).</li>
 * <li><b>I6 — fenced is terminal.</b> An instance that loses the chain never acknowledges again; only a new shard
 * incarnation, with a new instance, may re-seal.</li>
 * <li><b>I7 — one blob per shard.</b> All writers for a shard resolve the same key, and the blob content records the
 * shard identity, validated on every read ({@link #readRemoteState}) so a key collision fails loudly instead of
 * letting two shards fence each other.</li>
 * <li><b>I8 — the owner is advisory.</b> Ownership fields (allocation id, node id) are recorded for the audit trail
 * and diagnostics only; no authorization decision reads them, which is what lets a legitimate brand-new copy (e.g.
 * from a snapshot restore) take the chain.</li>
 * </ol>
 *
 * @opensearch.internal
 */
public class RemoteStoreFence {

    public static final String FENCE_BLOB_NAME = "fence";
    private static final String CODEC_VERSION = "v1";
    private static final String FIELD_SEPARATOR = "|";

    private final BlobContainer blobContainer;
    private final String ownerAllocationId;
    private final String ownerNodeId;
    private final ShardId shardId;
    private final ThreadPool threadPool;
    private final Logger logger;

    // Guarded by synchronized methods. Uploads on the ack path are effectively serialized by the translog sync
    // permits, but the fence must never issue two CAS calls with the same token.
    private String versionToken;
    private long term = -1;
    private long seq = -1;

    public RemoteStoreFence(
        BlobContainer blobContainer,
        String ownerAllocationId,
        String ownerNodeId,
        ShardId shardId,
        ThreadPool threadPool
    ) {
        this.blobContainer = blobContainer;
        this.ownerAllocationId = Objects.requireNonNull(ownerAllocationId, "fence owner allocation id");
        this.ownerNodeId = Objects.requireNonNull(ownerNodeId, "fence owner node id");
        this.shardId = shardId;
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
     * translog upload before the metadata file is published (invariant I1). The first call bootstraps the fence: it
     * creates the blob if absent, or seals it over to this owner if the supplied primary term is at least the
     * fence's term.
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
        // Invariant I4: the fence term never regresses.
        if (primaryTerm < term) {
            throw new TranslogFencedException(
                String.format(Locale.ROOT, "primary fenced by remote store: local term [%d] < fence term [%d]", primaryTerm, term)
            );
        }
        try {
            cas(versionToken, primaryTerm, seq + 1);
        } catch (BlobVersionConflictException e) {
            // Invariant I6: we lost the CAS chain and this instance is permanently fenced. Re-read to report who.
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
                FenceState remote = readRemoteState(blob.content());
                // Invariant I4: refuse to claim below the fence term.
                if (remote.term > primaryTerm) {
                    throw new TranslogFencedException(
                        String.format(
                            Locale.ROOT,
                            "primary fenced by remote store: local term [%d] < fence term [%d] owned by [%s]",
                            primaryTerm,
                            remote.term,
                            remote.describeOwner()
                        )
                    );
                }
                // remote.term == primaryTerm with a different owner is a legitimate takeover, not a conflict
                // (invariants I4 and I8): primary relocation hands the shard over at a constant term, so the target
                // must be able to claim the chain. Treating it as a fence here would make relocation impossible.
                // Ordering between the two copies is still enforced by the CAS: whoever claims the chain second wins,
                // and the loser fails its next upload. A relocation that is claimed and then aborted therefore fences
                // the source, which is safe (the shard fails and is reassigned) but coarse; wiring the handoff
                // through an explicit ownership CAS is a follow-up.
                expectedToken = blob.versionToken();
                nextSeq = remote.seq + 1;
            } else {
                expectedToken = null;
                nextSeq = 0;
            }
            try {
                cas(expectedToken, primaryTerm, nextSeq);
                logger.info(
                    "Fence bootstrapped: term [{}] allocation [{}] node [{}] seq [{}]",
                    primaryTerm,
                    ownerAllocationId,
                    ownerNodeId,
                    nextSeq
                );
                return;
            } catch (BlobVersionConflictException e) {
                logger.info("Fence bootstrap CAS conflict on attempt [{}], retrying", attempt + 1);
            }
        }
        throw fencedException(null);
    }

    /**
     * Parses the fence blob and validates that it describes this shard (invariant I7): a fence blob for a different
     * shard means two shards resolved the same key, which must fail loudly rather than let them fence each other.
     */
    private FenceState readRemoteState(byte[] content) throws IOException {
        FenceState remote = FenceState.parse(content);
        if (remote.indexUUID.equals(shardId.getIndex().getUUID()) == false || remote.shardId != shardId.id()) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Fence blob belongs to a different shard: expected index uuid [%s] shard [%d] but found [%s][%d];"
                        + " two shards resolved the same fence path",
                    shardId.getIndex().getUUID(),
                    shardId.id(),
                    remote.indexUUID,
                    remote.shardId
                )
            );
        }
        return remote;
    }

    private void cas(String expectedToken, long primaryTerm, long nextSeq) throws IOException {
        // Invariant I5: seq strictly increases along the chain.
        assert nextSeq > seq : "fence seq must advance: [" + seq + "] -> [" + nextSeq + "]";
        byte[] content = new FenceState(shardId.getIndex().getUUID(), shardId.id(), primaryTerm, ownerAllocationId, ownerNodeId, nextSeq)
            .toBytes();
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
            FenceState remote = readRemoteState(blobContainer.readBlobWithVersion(FENCE_BLOB_NAME).content());
            remoteDescription = String.format(
                Locale.ROOT,
                "term [%d] owner [%s] seq [%d]",
                remote.term,
                remote.describeOwner(),
                remote.seq
            );
        } catch (Exception e) {
            remoteDescription = "unreadable";
        }
        TranslogFencedException exception = new TranslogFencedException(
            String.format(
                Locale.ROOT,
                "primary fenced by remote store: fence owned by another writer [%s], local allocation [%s] node [%s] term [%d]",
                remoteDescription,
                ownerAllocationId,
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
     * Fence blob content: {@code v1|<indexUUID>|<shardId>|<term>|<allocationId>|<nodeId>|<seq>} in UTF-8.
     * <p>
     * The index UUID and shard id make the blob self-describing and are validated on every read (invariant I7). The
     * allocation id identifies the owning shard <i>copy</i> — the identity the cluster's in-sync set is expressed in —
     * and the node id is recorded alongside it for operators; both are advisory (invariant I8), since exclusion is
     * enforced entirely by the CAS chain.
     */
    static final class FenceState {
        final String indexUUID;
        final int shardId;
        final long term;
        final String allocationId;
        final String nodeId;
        final long seq;

        FenceState(String indexUUID, int shardId, long term, String allocationId, String nodeId, long seq) {
            // Index UUIDs, allocation ids and node ids are base64 UUIDs today, so this cannot fire; encoding a field
            // containing the separator would silently produce a blob that parse() rejects, which would fence a
            // healthy primary.
            for (String field : new String[] { indexUUID, allocationId, nodeId }) {
                if (field.contains(FIELD_SEPARATOR)) {
                    throw new IllegalArgumentException("Fence field [" + field + "] must not contain [" + FIELD_SEPARATOR + "]");
                }
            }
            this.indexUUID = indexUUID;
            this.shardId = shardId;
            this.term = term;
            this.allocationId = allocationId;
            this.nodeId = nodeId;
            this.seq = seq;
        }

        String describeOwner() {
            return String.format(Locale.ROOT, "allocation [%s] node [%s]", allocationId, nodeId);
        }

        byte[] toBytes() {
            return String.join(
                FIELD_SEPARATOR,
                CODEC_VERSION,
                indexUUID,
                Integer.toString(shardId),
                Long.toString(term),
                allocationId,
                nodeId,
                Long.toString(seq)
            ).getBytes(StandardCharsets.UTF_8);
        }

        static FenceState parse(byte[] content) throws IOException {
            String[] tokens = new String(content, StandardCharsets.UTF_8).split("\\" + FIELD_SEPARATOR);
            if (tokens.length != 7 || CODEC_VERSION.equals(tokens[0]) == false) {
                throw new IOException("Unrecognized fence blob content");
            }
            try {
                return new FenceState(
                    tokens[1],
                    Integer.parseInt(tokens[2]),
                    Long.parseLong(tokens[3]),
                    tokens[4],
                    tokens[5],
                    Long.parseLong(tokens[6])
                );
            } catch (NumberFormatException e) {
                throw new IOException("Unrecognized fence blob content", e);
            }
        }
    }
}
