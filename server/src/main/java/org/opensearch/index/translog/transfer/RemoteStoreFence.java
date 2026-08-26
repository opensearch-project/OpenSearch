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
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Object-store-backed primary fencing for a shard. Keeps one small mutable {@code fence__<term>} object per primary
 * term in the translog repository, written only by compare-and-swap
 * ({@link BlobContainer#writeBlobConditionally}). The term is inverted in the name, so a prefix listing returns the
 * highest term first. That ordering matters because an object store cannot interpret primary terms, and the name is
 * the one place it can compare them. So the cluster manager authorizes a takeover without doing any I/O itself.
 * <p>
 * <b>CAS, and the CAS chain.</b> Defined here because the rest of this class, and the TLA+ specifications under
 * {@code formal-models/remote-store-fence/}, are written in these terms.
 * <ul>
 * <li>A <b>CAS</b> - compare-and-swap - is a write the store accepts only if the object's current version is the one
 * the caller presents, and <em>refuses</em> otherwise rather than applying it. Object stores expose this as a
 * precondition on the write: S3 {@code If-Match}, GCS generation preconditions, Azure ETags. The version itself is an
 * opaque <b>version token</b>; this class never interprets it. Presenting no token means "only if absent", which is
 * how a path is first created.</li>
 * <li>The <b>CAS chain</b> is the succession of tokens one fence object passes through. A successful write must
 * present the current token and hands back the next one, so the links cannot be skipped: <em>holding the current
 * token is what it means to own the object</em>, and a writer that misses one link can never rejoin the chain. That
 * is the whole mechanism - staleness is detectable because there is no way to write without proving you saw the
 * previous state.</li>
 * </ul>
 * The fencing token is therefore this chain, not the primary term. Every acknowledged translog upload advances it
 * through {@link #validateAndAdvance}, so a second writer holds a stale token and its next CAS fails with a
 * {@link TranslogFencedException} before the write is acknowledged. That covers both a stale primary after failover
 * and a zombie relocation source. On top of that, a successor deletes the superseded writer's object, and it is the
 * delete rather than the CAS that makes a higher-term takeover certain instead of a race - a CAS alone would only be
 * a fair race between the two.
 * <p>
 * The CAS runs <b>concurrently</b> with the immutable translog metadata upload so it stays off the latency path, and
 * the upload is acknowledged only once both succeed. One consequence: a fenced writer can leave a single orphan
 * metadata file behind. It was never acknowledged, it is term-scoped, and readers that follow the highest-term
 * lineage ignore it. Cleaning those up is a follow-up.
 * <p>
 * The fence blob sits on the <b>control flow</b>: it carries decisions about who may write, and never any shard
 * data. Shard data travels on two other paths, named separately throughout because they are fenced by different
 * means — the <b>translog flow</b>, which the CAS gates directly, and the <b>segment flow</b>, which an ownership
 * check gates. Neither reads the fence, so snapshot restore, pinned-timestamp resolution and GC never touch it, and
 * every snapshot-referenced file stays immutable under an unchanged name.
 * <p>
 * A recovering primary claims the fence <b>before</b> it reads its translog restore point (see
 * {@code RemoteFsTranslog#sealFence}), and that ordering is what closes the acked-write-loss window on failover. A
 * previous primary that is still running but has dropped out of the cluster's view loses its object at that moment,
 * so it can acknowledge nothing that lands after the restore point the new copy read. A copy taking a primary
 * relocation handoff does not seal, because its source is still serving legitimately at the same term. Sharing a
 * term means sharing an object, so ordering between those two is the handoff protocol's job, not the fence's.
 * <p>
 * <b>Invariants.</b> Referenced by name from the code that enforces them:
 * <ul>
 * <li><b>The chain gates the ack.</b> An upload is acknowledged only if its fence CAS succeeded.
 * {@code TranslogTransferManager#transferSnapshot} joins the CAS before it acknowledges.</li>
 * <li><b>One writer at a time.</b> Any successful fence write invalidates every other holder's token. The
 * repository's conditional writes are what provide this, so {@code RemoteFsTranslog#buildFence} refuses a repository
 * that lacks them rather than running unfenced.</li>
 * <li><b>Seal before restore.</b> Every writer transition claims the chain before it reads the translog restore
 * point it will serve from, or before it destroys remote translog state as a snapshot restore does. The one
 * exception is a same-term relocation handoff. This holds on every takeover path: failover promotion, store and
 * remote-store recovery, and snapshot restore, including snapshot V2, where the restore point is downloaded when the
 * translog is built. Once the previous writer's token is invalid, everything it acknowledged is already in the
 * restore point the new copy read, and nothing it tries afterwards can be acknowledged. That is what covers a
 * previous writer stuck behind a network partition. Asserted at the read choke point in
 * {@code IndexShard#syncRemoteTranslogAndUpdateGlobalCheckpoint}.</li>
 * <li><b>The term never regresses.</b> {@link #claim} refuses a claimant below the highest term that owns an
 * acknowledgement path. An equal term may still claim, because relocation and in-place snapshot restore are both
 * legitimate equal-term takeovers; {@link #arbitrateSameTerm} settles those by CAS instead of by term.</li>
 * <li><b>A higher term prevails, deterministically.</b> Every step of {@link #claim} is either uncontested by the
 * incumbent, being a create-if-absent on a key only this term's owner writes, or unconditional, being a delete of
 * strictly lower terms. No step is decided by winning a race. A writer's only destructive act is deleting objects
 * below its own term, so it can never touch a higher-term writer's path. Model checking refuted the obvious
 * alternative, a single shared blob escalating to an unconditional overwrite: the listing it takes before escalating
 * can go stale, which lets a lower-term writer fence the rightful higher-term owner. See
 * {@code FenceTakeover.tla}.</li>
 * <li><b>Seq strictly increases along a term's chain</b> (asserted in {@link #cas}).</li>
 * <li><b>Fenced is terminal.</b> An instance whose path was taken over or deleted never acknowledges again. Only a
 * new shard incarnation, with a new instance, may claim again.</li>
 * <li><b>One object per term, keyed by index UUID.</b> Every writer at a given term resolves the same key, and the
 * content records the shard identity, checked on every read ({@link #readRemoteState}), so a path collision fails
 * loudly instead of letting two shards fence each other. The index UUID in the path isolates index lifecycle
 * operations. A resize target, whether shrink, split or clone, is a new index with its own UUID, so its shards claim
 * fresh keys and never contend with the write-blocked source. A restore into a new index UUID starts fresh the same
 * way, and a partitioned writer of the incarnation being replaced can only touch keys nothing will read again.</li>
 * <li><b>The owner is advisory.</b> The allocation id and node id are recorded for the audit trail and for
 * diagnostics. No authorization decision reads them, which is what lets a legitimate brand-new copy, such as one
 * from a snapshot restore, take the chain.</li>
 * <li><b>No cluster-manager synchronization.</b> The cluster manager neither reads nor writes the fence, and nothing
 * has to be synced back to it. The fence borrows just two properties from cluster coordination. First, primary terms
 * are issued monotonically, so a primary appointed after a failure carries a strictly higher term; restore keeps
 * this true by taking the max of the snapshot's term and the current metadata's. Second, the routing table admits at
 * most one active primary per shard, so two cluster-sanctioned same-term writers only ever exist during a relocation
 * handoff. Everything else is enforced on the object-store side, which is the point: fencing has to keep working
 * when the writer cannot reach the cluster manager at all.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class RemoteStoreFence {

    /**
     * Prefix of the per-term acknowledgement-path objects. Deliberately neither a prefix of nor prefixed by
     * {@code TranslogTransferMetadata.METADATA_PREFIX}, so metadata listings, restore lineage and GC never see these.
     */
    public static final String FENCE_BLOB_PREFIX = "fence__";
    private static final String CODEC_VERSION = "v1";
    private static final String FIELD_SEPARATOR = "|";
    /**
     * Bound on equal-term arbitration attempts. Only equal-term claimants (a relocation source and its target, or an
     * in-place restore) contend for an existing object, and losing repeatedly to an equal-term writer is a legitimate
     * arbitration loss rather than a race worth outlasting.
     */
    private static final int MAX_SAME_TERM_ATTEMPTS = 2;

    /**
     * The acknowledgement-path object for {@code term}. The term is inverted so that a plain prefix listing returns the
     * highest term first - which is what lets the object store order cluster-manager-issued grants it cannot otherwise
     * interpret.
     */
    public static String fenceBlobName(long term) {
        return FENCE_BLOB_PREFIX + RemoteStoreUtils.invertLong(term);
    }

    private static long termOf(String blobName) {
        return RemoteStoreUtils.invertLong(blobName.substring(FENCE_BLOB_PREFIX.length()));
    }

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
    /**
     * Set once this instance has been fenced, and never cleared. Terminality has to be explicit state rather than a
     * consequence of holding a stale token: a fenced instance asked to act at a <i>different</i> term must not be able
     * to claim that term's path, since it was never granted it - doing so would let it delete its own successor's
     * object and resurrect itself.
     */
    private boolean fenced;
    /**
     * Set for a primary relocation target. Such a copy shares a term - and therefore an object - with a source that is
     * still legitimately serving, so it may only take the chain over when the source has explicitly handed ownership to
     * it ({@link #transferOwnershipTo}). Claiming blindly is what fences a healthy source whose handoff later aborts.
     * Other equal-term claimants, such as a restore over a closed index, keep the ordinary arbitration.
     */
    private final boolean requireTransferredOwnership;

    public RemoteStoreFence(
        BlobContainer blobContainer,
        String ownerAllocationId,
        String ownerNodeId,
        ShardId shardId,
        ThreadPool threadPool
    ) {
        this(blobContainer, ownerAllocationId, ownerNodeId, shardId, threadPool, false);
    }

    public RemoteStoreFence(
        BlobContainer blobContainer,
        String ownerAllocationId,
        String ownerNodeId,
        ShardId shardId,
        ThreadPool threadPool,
        boolean requireTransferredOwnership
    ) {
        this.requireTransferredOwnership = requireTransferredOwnership;
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
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            // Deliberately outside the catch. A listener that throws is a bug in the listener, not a fence failure, and
            // routing it to onFailure would both deliver two terminal callbacks and misreport a CAS that actually
            // succeeded as an upload failure. Let it reach the executor's uncaught handler instead.
            listener.onResponse(null);
        });
    }

    /**
     * Validates that this shard copy still owns its term's acknowledgement path and advances the CAS chain. Must be
     * called on every translog upload before the metadata file is published (invariant: the chain gates the ack). The
     * first call claims the path for {@code primaryTerm} (see {@link #claim}); every later call advances the chain with
     * the token retained from the previous write.
     *
     * @param primaryTerm the primary term this upload is being performed at
     * @throws TranslogFencedException if another shard copy owns the fence — fatal, the caller must stop acking
     * @throws IOException on repository errors
     */
    public synchronized void validateAndAdvance(long primaryTerm) throws IOException {
        if (fenced) {
            throw fencedException(null);
        }
        if (versionToken == null) {
            claim(primaryTerm);
            return;
        }
        if (primaryTerm != term) {
            // A fence instance belongs to one shard copy at one term - a term change means a new engine, and so a new
            // fence. Seeing a different term here is unexpected, so fail closed rather than claim a term this instance
            // was never granted.
            fenced = true;
            throw new TranslogFencedException(
                String.format(
                    Locale.ROOT,
                    "primary fenced by remote store: fence for term [%d] asked to acknowledge at term [%d]",
                    term,
                    primaryTerm
                )
            );
        }
        final long attemptedSeq = seq + 1;
        try {
            cas(primaryTerm, versionToken, attemptedSeq);
        } catch (BlobVersionConflictException e) {
            // Fenced is terminal: our acknowledgement path was either taken over by an equal-term twin or deleted by a
            // higher-term successor, so this instance can never acknowledge again. Re-read to report who superseded us.
            throw fencedException(e);
        } catch (IOException e) {
            // Ambiguous rather than failed: the conditional write may have landed with only the response lost. Our
            // token would then be stale, and the next attempt's If-Match would fail and be indistinguishable from
            // being fenced - failing a healthy primary on a network blip. Resolve it by reading who the object says
            // wrote it. Not treating this as terminal is what keeps the error retryable.
            if (adoptOwnWriteIfItLanded(primaryTerm, attemptedSeq) == false) {
                throw e;
            }
        }
    }

    /**
     * Resolves an ambiguous conditional write: did our own CAS land before the response was lost?
     * <p>
     * Answers by identity rather than by token, which is the only way to tell. The object records the writing copy's
     * term, allocation id and node id along with its seq, so a blob carrying ours at exactly the seq we attempted can
     * only be the write we just issued. In that case adopt the token it now has and carry on; the acknowledgement is
     * legitimate. Anything else - a different writer, a different seq, an unreadable or missing object - is reported as
     * "did not land", which is the retryable direction and never fences.
     */
    private boolean adoptOwnWriteIfItLanded(long primaryTerm, long attemptedSeq) {
        try {
            VersionedBlob current = blobContainer.readBlobWithVersion(fenceBlobName(primaryTerm));
            FenceState remote = readRemoteState(current.content());
            if (remote.seq == attemptedSeq
                && remote.term == primaryTerm
                && ownerAllocationId.equals(remote.allocationId)
                && ownerNodeId.equals(remote.nodeId)) {
                this.versionToken = current.versionToken();
                this.term = primaryTerm;
                this.seq = attemptedSeq;
                logger.info(
                    "Fence write at term [{}] seq [{}] had landed; adopted its token after a lost response",
                    primaryTerm,
                    attemptedSeq
                );
                return true;
            }
        } catch (Exception unresolved) {
            // Cannot tell, so report the retryable answer rather than guessing.
            logger.warn("Could not determine whether the fence write at term [" + primaryTerm + "] landed", unresolved);
        }
        return false;
    }

    /**
     * Claims the acknowledgement path for {@code primaryTerm}, deterministically with respect to the incumbent.
     * <p>
     * Every step is either uncontested by the incumbent or unconditional, which is what makes a higher-term takeover
     * guaranteed rather than probabilistic:
     * <ol>
     * <li><b>List.</b> A strictly higher term already owning a path means this copy has been superseded - refuse.</li>
     * <li><b>Create</b> our own {@code fence__<term>} with create-if-absent. A lower-term incumbent never writes this
     * key, so it cannot defeat the create. A conflict means an equal-term writer already owns it - a relocation
     * source/target pair, or an in-place restore - which falls through to ordinary CAS arbitration.</li>
     * <li><b>Delete every lower-term object.</b> Unconditional, so it always succeeds: this is the act that fences the
     * incumbent, whose next CAS then finds its object gone.</li>
     * <li><b>List again.</b> A grant issued during our window supersedes ours: withdraw our own object and refuse, so
     * a stale grant is self-limiting rather than able to displace the rightful owner.</li>
     * </ol>
     * A writer's only destructive act is deleting objects strictly below its own term, so it can never touch a
     * higher-term writer's acknowledgement path. Determinism therefore follows from the key space rather than from
     * winning a race - model-checked in {@code FenceTakeover.tla}.
     */
    private void claim(long primaryTerm) throws IOException {
        long higher = highestTerm(primaryTerm);
        if (higher > primaryTerm) {
            fenced = true;
            throw new TranslogFencedException(
                String.format(Locale.ROOT, "primary fenced by remote store: term [%d] superseded by term [%d]", primaryTerm, higher)
            );
        }

        boolean created;
        try {
            cas(primaryTerm, null, 0);
            created = true;
        } catch (BlobVersionConflictException e) {
            // Arbitration takes an existing object over; it never creates one, so the path is not ours to withdraw.
            created = false;
            arbitrateSameTerm(primaryTerm);
        }

        deleteTermsBelow(primaryTerm);

        // Re-assert that we still hold our own path, which is VerifyClaim's wHeld = pathToken guard. One GET, and
        // claims happen on recovery, promotion and relocation rather than per operation. Detecting an equal-term twin
        // here rather than at the first CAS also stops a doomed copy hydrating segments and replaying a translog it
        // will never serve, and stops us withdrawing below a path that twin now owns.
        try {
            VersionedBlob current = blobContainer.readBlobWithVersion(fenceBlobName(primaryTerm));
            if (versionToken.equals(current.versionToken()) == false) {
                FenceState twin = readRemoteState(current.content());
                fenced = true;
                throw new TranslogFencedException(
                    String.format(
                        Locale.ROOT,
                        "primary fenced by remote store: the acknowledgement path for term [%d] was taken over mid-claim by [%s]",
                        primaryTerm,
                        twin.describeOwner()
                    )
                );
            }
        } catch (NoSuchFileException e) {
            // Our own path is gone, so a successor swept it: the same outcome as losing the CAS.
            throw fencedException(e);
        }

        // One listing, two checks. Both matter, and the second is not belt-and-braces: a batch delete reports per-key
        // failures in its response body rather than throwing, and the shared blob-store helper logs them and returns
        // success - so a "successful" sweep can leave a lower-term path standing. That copy still holds a valid token
        // and would keep acknowledging, so proceeding would put two terms on the write path at once. Refuse instead,
        // retryably: nothing has been acknowledged at this point.
        Set<Long> remaining = listTerms();
        Optional<Long> survivor = remaining.stream().filter(t -> t < primaryTerm).findFirst();
        higher = remaining.stream().max(Long::compareTo).orElse(primaryTerm);
        // Order follows VerifyClaim in FenceTakeover.tla, where no-surviving-lower-term is an unconditional
        // precondition of completing a claim rather than a test on one branch of it. Checking it first can cost a
        // wasted retry when a higher term is also present, which is a price worth paying to keep "the sweep landed"
        // a precondition in both the spec and the code.
        if (survivor.isPresent()) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Fence sweep left the acknowledgement path for superseded term [%d] in place while claiming term [%d]; retry",
                    survivor.get(),
                    primaryTerm
                )
            );
        }
        if (higher > primaryTerm) {
            // Withdraw only a path this copy CREATED - see withdraw(). Either way the higher term's sweep removes it,
            // so withdrawing is a courtesy that saves that successor a retry, never a requirement.
            withdraw(primaryTerm, created);
            fenced = true;
            throw new TranslogFencedException(
                String.format(
                    Locale.ROOT,
                    "primary fenced by remote store: term [%d] superseded by term [%d] mid-claim",
                    primaryTerm,
                    higher
                )
            );
        }
        logger.info(
            "Fence claimed: term [{}] allocation [{}] node [{}] created [{}]",
            primaryTerm,
            ownerAllocationId,
            ownerNodeId,
            created
        );
    }

    /**
     * Whether a strictly higher primary term now owns an acknowledgement path for this shard - that is, whether this
     * copy has been superseded.
     * <p>
     * Deliberately a POSITIVE test for supersession rather than a test that this copy still holds its own object, and
     * deliberately independent of this instance's cached state. Used to gate operations that mutate shared remote state
     * without being on the acknowledgement path: publishing segment metadata, and garbage collection on either plane.
     * <p>
     * The distinction matters. A copy can legitimately hold a fence instance that is BEHIND the fence object - engine
     * resets during recovery replace the translog, and so the fence, several times in quick succession - and such an
     * instance is stale, not superseded. Asking "do I still hold my own object?" answers false for both cases and
     * wrongly silences a healthy shard; asking "does a higher term exist?" separates them.
     * <p>
     * A read, not a CAS: these paths need "stop touching shared state once superseded", not mutual exclusion, and a CAS
     * here would contend with the acknowledgement path for the same chain and fence the shard against itself.
     */
    public boolean isSuperseded(long primaryTerm) throws IOException {
        return highestTerm(primaryTerm) > primaryTerm;
    }

    /** The terms that currently own an acknowledgement path for this shard. */
    private Set<Long> listTerms() throws IOException {
        Set<Long> terms = new HashSet<>();
        for (String blobName : blobContainer.listBlobsByPrefix(FENCE_BLOB_PREFIX).keySet()) {
            terms.add(termOf(blobName));
        }
        return terms;
    }

    /** The highest term owning an acknowledgement path, or {@code floor} when none is above it. */
    private long highestTerm(long floor) throws IOException {
        return listTerms().stream().max(Long::compareTo).filter(t -> t > floor).orElse(floor);
    }

    /**
     * Equal-term arbitration. A relocation source and its target legitimately share a term, so whichever CASes the
     * shared object last owns it and the other fails its next upload. Same-term ordering is the handoff protocol's
     * responsibility, not the fence's.
     */
    /**
     * Settles a contest with an equal-term twin for an object that already exists. Never creates the object, so a path
     * won this way is never ours to withdraw - see {@link #withdraw}.
     */
    private void arbitrateSameTerm(long primaryTerm) throws IOException {
        for (int attempt = 0; attempt < MAX_SAME_TERM_ATTEMPTS; attempt++) {
            VersionedBlob blob;
            try {
                blob = blobContainer.readBlobWithVersion(fenceBlobName(primaryTerm));
            } catch (NoSuchFileException e) {
                if (requireTransferredOwnership) {
                    fenced = true;
                    throw new TranslogFencedException(
                        String.format(Locale.ROOT, "primary relocation target found no fence to adopt at term [%d]", primaryTerm)
                    );
                }
                // Nothing left to arbitrate for: the twin withdrew, or a successor swept the path. Fence, as
                // ArbitrateSameTerm does in FenceTakeover.tla, rather than recreating the object. Recreating would be
                // reachable legitimately only when the twin genuinely withdrew, and it buys nothing there - this copy
                // is reassigned at a higher term and claims cleanly then - while putting a create-if-absent on a path
                // a successor may just have swept, which the spec does not sanction.
                fenced = true;
                throw new TranslogFencedException(
                    String.format(
                        Locale.ROOT,
                        "primary fenced by remote store: the acknowledgement path for term [%d] no longer exists to arbitrate for",
                        primaryTerm
                    )
                );
            }
            FenceState remote = readRemoteState(blob.content());
            if (requireTransferredOwnership && ownerAllocationId.equals(remote.allocationId) == false) {
                // A relocation source either has not handed ownership over yet, or has reclaimed it after aborting the
                // handoff. Either way this copy is not the sanctioned owner and must not take the chain: doing so would
                // fence a source that is still serving.
                fenced = true;
                throw new TranslogFencedException(
                    String.format(
                        Locale.ROOT,
                        "primary relocation target not granted the fence at term [%d]: owned by [%s]",
                        primaryTerm,
                        remote.describeOwner()
                    )
                );
            }
            try {
                cas(primaryTerm, blob.versionToken(), remote.seq + 1);
                return; // taken over, not created: never ours to withdraw
            } catch (BlobVersionConflictException e) {
                logger.info("Fence equal-term arbitration conflict on attempt [{}], retrying", attempt + 1);
            }
        }
        throw fencedException(null);
    }

    /**
     * Hands ownership of this term's acknowledgement path to {@code targetAllocationId}, as the final act of a primary
     * relocation handoff.
     * <p>
     * Performed by the copy that currently owns the chain, after its uploads have drained, so it is uncontested. The
     * token this produces is retained, which is what makes {@link #revertOwnership} able to distinguish a target that
     * took over from one that never wrote - the ambiguity the source and target cannot resolve between themselves.
     */
    public synchronized void transferOwnershipTo(long primaryTerm, String targetAllocationId) throws IOException {
        if (fenced) {
            throw fencedException(null);
        }
        if (versionToken == null || primaryTerm != term) {
            throw new TranslogFencedException(
                String.format(
                    Locale.ROOT,
                    "cannot hand off the fence at term [%d]: this copy does not own the chain (fence term [%d])",
                    primaryTerm,
                    term
                )
            );
        }
        try {
            cas(primaryTerm, versionToken, seq + 1, targetAllocationId);
        } catch (BlobVersionConflictException e) {
            throw fencedException(e);
        }
        logger.info("Fence ownership handed to allocation [{}] at term [{}]", targetAllocationId, primaryTerm);
    }

    /**
     * Reclaims ownership after an aborted relocation handoff, using the token produced by
     * {@link #transferOwnershipTo}.
     *
     * @return {@code true} when ownership was reclaimed, meaning the target never wrote and so never took over - this
     *         copy may resume as primary; {@code false} when the target had already written, meaning the handoff
     *         effectively completed and this copy must stand down. Standing down is not a fencing error.
     */
    public synchronized boolean revertOwnership(long primaryTerm) throws IOException {
        if (fenced || versionToken == null || primaryTerm != term) {
            return false;
        }
        try {
            cas(primaryTerm, versionToken, seq + 1, ownerAllocationId);
        } catch (BlobVersionConflictException e) {
            logger.info(
                "Fence ownership at term [{}] is no longer ours - the target took it up, or a higher term swept it; standing down",
                primaryTerm
            );
            return false;
        }
        logger.info("Fence ownership reclaimed at term [{}] after an aborted relocation handoff", primaryTerm);
        return true;
    }

    /** Unconditional deletes - the act that fences a lower-term incumbent. */
    private void deleteTermsBelow(long primaryTerm) throws IOException {
        List<String> superseded = new ArrayList<>();
        for (String blobName : blobContainer.listBlobsByPrefix(FENCE_BLOB_PREFIX).keySet()) {
            if (termOf(blobName) < primaryTerm) {
                superseded.add(blobName);
            }
        }
        if (superseded.isEmpty() == false) {
            blobContainer.deleteBlobsIgnoringIfNotExists(superseded);
            logger.info("Fence removed superseded acknowledgement paths {} on claiming term [{}]", superseded, primaryTerm);
        }
    }

    /** Withdraw our own acknowledgement path, so a superseded claim leaves nothing behind. */
    private void withdraw(long primaryTerm, boolean createdByThisCopy) {
        // Delete only an object this copy created. Winning one from an equal-term twin does not make it ours to remove:
        // a further equal-term claimant may have taken it over since, and our token would be stale without us knowing.
        // This is also the single place a writer would delete at its OWN term, the one exception to "a writer's only
        // destructive act is deleting strictly BELOW its own term" that the rest of the protocol leans on.
        // FenceTakeover.tla draws the same distinction: VerifyClaim withdraws only IF wCreated, and the variable is
        // commented "whether this copy CREATED its own path (so whether it may withdraw it)".
        if (createdByThisCopy) {
            try {
                blobContainer.deleteBlobsIgnoringIfNotExists(List.of(fenceBlobName(primaryTerm)));
            } catch (IOException e) {
                // Best effort: a successor deletes every lower-term path anyway, so a leftover is transient.
                logger.warn("Failed to withdraw the fence for superseded term [" + primaryTerm + "]", e);
            }
        }
        // Local state is released either way: this instance is fenced and must never acknowledge again.
        versionToken = null;
        term = -1;
        seq = -1;
    }

    /**
     * Parses the fence blob and validates that it describes this shard (invariant: one blob per shard): a fence blob for a different
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

    private void cas(long primaryTerm, String expectedToken, long nextSeq) throws IOException {
        cas(primaryTerm, expectedToken, nextSeq, ownerAllocationId);
    }

    private void cas(long primaryTerm, String expectedToken, long nextSeq, String recordedAllocationId) throws IOException {
        // Invariant: seq strictly increases along a term's chain.
        assert primaryTerm != term || nextSeq > seq : "fence seq must advance: [" + seq + "] -> [" + nextSeq + "]";
        byte[] content = new FenceState(shardId.getIndex().getUUID(), shardId.id(), primaryTerm, recordedAllocationId, ownerNodeId, nextSeq)
            .toBytes();
        String newToken = blobContainer.writeBlobConditionally(
            fenceBlobName(primaryTerm),
            new ByteArrayInputStream(content),
            content.length,
            expectedToken
        );
        this.versionToken = newToken;
        this.term = primaryTerm;
        this.seq = nextSeq;
    }

    private TranslogFencedException fencedException(Exception cause) {
        fenced = true;
        String remoteDescription;
        try {
            // 0 rather than -1 as the floor: an empty listing must not produce a negative term, which has no encoding.
            long highest = highestTerm(0);
            FenceState remote = readRemoteState(blobContainer.readBlobWithVersion(fenceBlobName(highest)).content());
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
     * The index UUID and shard id make the blob self-describing and are validated on every read (one blob per shard). The
     * allocation id identifies the owning shard <i>copy</i> — the identity the cluster's in-sync set is expressed in —
     * and the node id is recorded alongside it for operators; both are advisory, since exclusion is
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
