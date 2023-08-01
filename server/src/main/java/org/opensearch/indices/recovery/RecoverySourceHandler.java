/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.StopWatch;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.common.util.concurrent.ListenableFuture;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.engine.RecoveryEngineException;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.RetentionLeaseNotFoundException;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.RunUnderPrimaryPermit;
import org.opensearch.indices.replication.SegmentFileTransferHandler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.StreamSupport;

/**
 * RecoverySourceHandler handles the three phases of shard recovery, which is
 * everything relating to copying the segment files as well as sending translog
 * operations across the wire once the segments have been copied.
 * <p>
 * Note: There is always one source handler per recovery that handles all the
 * file and translog transfer. This handler is completely isolated from other recoveries
 * while the {@link RateLimiter} passed via {@link RecoverySettings} is shared across recoveries
 * originating from this nodes to throttle the number bytes send during file transfer. The transaction log
 * phase bypasses the rate limiter entirely.
 *
 * @opensearch.internal
 */
public abstract class RecoverySourceHandler {

    protected final Logger logger;
    // Shard that is going to be recovered (the "source")
    protected final IndexShard shard;
    protected final int shardId;
    // Request containing source and target node information
    protected final StartRecoveryRequest request;
    private final int chunkSizeInBytes;
    private final RecoveryTargetHandler recoveryTarget;
    private final int maxConcurrentOperations;
    private final ThreadPool threadPool;
    protected final CancellableThreads cancellableThreads = new CancellableThreads();
    protected final List<Closeable> resources = new CopyOnWriteArrayList<>();
    protected final ListenableFuture<RecoveryResponse> future = new ListenableFuture<>();
    public static final String PEER_RECOVERY_NAME = "peer-recovery";
    private final SegmentFileTransferHandler transferHandler;

    RecoverySourceHandler(
        IndexShard shard,
        RecoveryTargetHandler recoveryTarget,
        ThreadPool threadPool,
        StartRecoveryRequest request,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks,
        int maxConcurrentOperations
    ) {
        this.logger = Loggers.getLogger(RecoverySourceHandler.class, request.shardId(), "recover to " + request.targetNode().getName());
        this.transferHandler = new SegmentFileTransferHandler(
            shard,
            request.targetNode(),
            recoveryTarget,
            logger,
            threadPool,
            cancellableThreads,
            fileChunkSizeInBytes,
            maxConcurrentFileChunks
        );
        this.shard = shard;
        this.threadPool = threadPool;
        this.request = request;
        this.recoveryTarget = recoveryTarget;
        this.shardId = this.request.shardId().id();
        this.chunkSizeInBytes = fileChunkSizeInBytes;
        // if the target is on an old version, it won't be able to handle out-of-order file chunks.
        this.maxConcurrentOperations = maxConcurrentOperations;
    }

    public StartRecoveryRequest getRequest() {
        return request;
    }

    public void addListener(ActionListener<RecoveryResponse> listener) {
        future.addListener(listener, OpenSearchExecutors.newDirectExecutorService());
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public void recoverToTarget(ActionListener<RecoveryResponse> listener) {
        addListener(listener);
        final Closeable releaseResources = () -> IOUtils.close(resources);
        try {
            cancellableThreads.setOnCancel((reason, beforeCancelEx) -> {
                final RuntimeException e;
                if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                    e = new IndexShardClosedException(shard.shardId(), "shard is closed and recovery was canceled reason [" + reason + "]");
                } else {
                    e = new CancellableThreads.ExecutionCancelledException("recovery was canceled reason [" + reason + "]");
                }
                if (beforeCancelEx != null) {
                    e.addSuppressed(beforeCancelEx);
                }
                IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
                throw e;
            });
            final Consumer<Exception> onFailure = e -> {
                assert Transports.assertNotTransportThread(this + "[onFailure]");
                IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
            };
            innerRecoveryToTarget(listener, onFailure);
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
        }
    }

    protected abstract void innerRecoveryToTarget(ActionListener<RecoveryResponse> listener, Consumer<Exception> onFailure)
        throws IOException;

    protected void finalizeStepAndCompleteFuture(
        long startingSeqNo,
        StepListener<SendSnapshotResult> sendSnapshotStep,
        StepListener<SendFileResult> sendFileStep,
        StepListener<TimeValue> prepareEngineStep,
        Consumer<Exception> onFailure
    ) {
        final StepListener<Void> finalizeStep = new StepListener<>();
        // Recovery target can trim all operations >= startingSeqNo as we have sent all these operations in the phase 2
        final long trimAboveSeqNo = startingSeqNo - 1;
        sendSnapshotStep.whenComplete(r -> finalizeRecovery(r.targetLocalCheckpoint, trimAboveSeqNo, finalizeStep), onFailure);

        finalizeStep.whenComplete(r -> {
            final long phase1ThrottlingWaitTime = 0L; // TODO: return the actual throttle time
            final SendSnapshotResult sendSnapshotResult = sendSnapshotStep.result();
            final SendFileResult sendFileResult = sendFileStep.result();
            final RecoveryResponse response = new RecoveryResponse(
                sendFileResult.phase1FileNames,
                sendFileResult.phase1FileSizes,
                sendFileResult.phase1ExistingFileNames,
                sendFileResult.phase1ExistingFileSizes,
                sendFileResult.totalSize,
                sendFileResult.existingTotalSize,
                sendFileResult.took.millis(),
                phase1ThrottlingWaitTime,
                prepareEngineStep.result().millis(),
                sendSnapshotResult.sentOperations,
                sendSnapshotResult.tookTime.millis()
            );
            try {
                future.onResponse(response);
            } finally {
                IOUtils.close(resources);
            }
        }, onFailure);
    }

    protected void onSendFileStepComplete(
        StepListener<SendFileResult> sendFileStep,
        GatedCloseable<IndexCommit> wrappedSafeCommit,
        Releasable releaseStore
    ) {
        sendFileStep.whenComplete(r -> IOUtils.close(wrappedSafeCommit, releaseStore), e -> {
            try {
                IOUtils.close(wrappedSafeCommit, releaseStore);
            } catch (final IOException ex) {
                logger.warn("releasing snapshot caused exception", ex);
            }
        });
    }

    protected boolean isTargetSameHistory() {
        final String targetHistoryUUID = request.metadataSnapshot().getHistoryUUID();
        assert targetHistoryUUID != null : "incoming target history missing";
        return targetHistoryUUID.equals(shard.getHistoryUUID());
    }

    /**
     * Counts the number of history operations from the starting sequence number
     *
     * @param startingSeqNo the starting sequence number to count; included
     * @return number of history operations
     */
    protected int countNumberOfHistoryOperations(long startingSeqNo) throws IOException {
        return shard.countNumberOfHistoryOperations(PEER_RECOVERY_NAME, startingSeqNo, Long.MAX_VALUE);
    }

    /**
     * Increases the store reference and returns a {@link Releasable} that will decrease the store reference using the generic thread pool.
     * We must never release the store using an interruptible thread as we can risk invalidating the node lock.
     */
    protected Releasable acquireStore(Store store) {
        store.incRef();
        return Releasables.releaseOnce(() -> runWithGenericThreadPool(store::decRef));
    }

    /**
     * Releasing a safe commit can access some commit files. It's better not to use {@link CancellableThreads} to interact
     * with the file systems due to interrupt (see {@link org.apache.lucene.store.NIOFSDirectory} javadocs for more detail).
     * This method acquires a safe commit and wraps it to make sure that it will be released using the generic thread pool.
     */
    protected GatedCloseable<IndexCommit> acquireSafeCommit(IndexShard shard) {
        final GatedCloseable<IndexCommit> wrappedSafeCommit = shard.acquireSafeIndexCommit();
        final AtomicBoolean closed = new AtomicBoolean(false);
        return new GatedCloseable<>(wrappedSafeCommit.get(), () -> {
            if (closed.compareAndSet(false, true)) {
                runWithGenericThreadPool(wrappedSafeCommit::close);
            }
        });
    }

    private void runWithGenericThreadPool(CheckedRunnable<Exception> task) {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        assert threadPool.generic().isShutdown() == false;
        // TODO: We shouldn't use the generic thread pool here as we already execute this from the generic pool.
        // While practically unlikely at a min pool size of 128 we could technically block the whole pool by waiting on futures
        // below and thus make it impossible for the store release to execute which in turn would block the futures forever
        threadPool.generic().execute(ActionRunnable.run(future, task));
        FutureUtils.get(future);
    }

    /**
     * A send file result
     *
     * @opensearch.internal
     */
    static final class SendFileResult {
        final List<String> phase1FileNames;
        final List<Long> phase1FileSizes;
        final long totalSize;

        final List<String> phase1ExistingFileNames;
        final List<Long> phase1ExistingFileSizes;
        final long existingTotalSize;

        final TimeValue took;

        SendFileResult(
            List<String> phase1FileNames,
            List<Long> phase1FileSizes,
            long totalSize,
            List<String> phase1ExistingFileNames,
            List<Long> phase1ExistingFileSizes,
            long existingTotalSize,
            TimeValue took
        ) {
            this.phase1FileNames = phase1FileNames;
            this.phase1FileSizes = phase1FileSizes;
            this.totalSize = totalSize;
            this.phase1ExistingFileNames = phase1ExistingFileNames;
            this.phase1ExistingFileSizes = phase1ExistingFileSizes;
            this.existingTotalSize = existingTotalSize;
            this.took = took;
        }

        static final SendFileResult EMPTY = new SendFileResult(
            Collections.emptyList(),
            Collections.emptyList(),
            0L,
            Collections.emptyList(),
            Collections.emptyList(),
            0L,
            TimeValue.ZERO
        );
    }

    /**
     * Perform phase1 of the recovery operations. Once this {@link IndexCommit}
     * snapshot has been performed no commit operations (files being fsync'd)
     * are effectively allowed on this index until all recovery phases are done
     * <p>
     * Phase1 examines the segment files on the target node and copies over the
     * segments that are missing. Only segments that have the same size and
     * checksum can be reused
     */
    void phase1(
        IndexCommit snapshot,
        long startingSeqNo,
        IntSupplier translogOps,
        ActionListener<SendFileResult> listener,
        boolean skipCreateRetentionLeaseStep
    ) {
        cancellableThreads.checkForCancel();
        final Store store = shard.store();
        try {
            StopWatch stopWatch = new StopWatch().start();
            final Store.MetadataSnapshot recoverySourceMetadata;
            try {
                recoverySourceMetadata = store.getMetadata(snapshot);
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                shard.failShard("recovery", ex);
                throw ex;
            }
            for (String name : snapshot.getFileNames()) {
                final StoreFileMetadata md = recoverySourceMetadata.get(name);
                if (md == null) {
                    logger.info("Snapshot differs from actual index for file: {} meta: {}", name, recoverySourceMetadata.asMap());
                    throw new CorruptIndexException(
                        "Snapshot differs from actual index - maybe index was removed metadata has "
                            + recoverySourceMetadata.asMap().size()
                            + " files",
                        name
                    );
                }
            }
            if (canSkipPhase1(recoverySourceMetadata, request.metadataSnapshot()) == false) {
                final List<String> phase1FileNames = new ArrayList<>();
                final List<Long> phase1FileSizes = new ArrayList<>();
                final List<String> phase1ExistingFileNames = new ArrayList<>();
                final List<Long> phase1ExistingFileSizes = new ArrayList<>();

                // Total size of segment files that are recovered
                long totalSizeInBytes = 0;
                // Total size of segment files that were able to be re-used
                long existingTotalSizeInBytes = 0;

                // Generate a "diff" of all the identical, different, and missing
                // segment files on the target node, using the existing files on
                // the source node
                final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(request.metadataSnapshot());
                for (StoreFileMetadata md : diff.identical) {
                    phase1ExistingFileNames.add(md.name());
                    phase1ExistingFileSizes.add(md.length());
                    existingTotalSizeInBytes += md.length();
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "recovery [phase1]: not recovering [{}], exist in local store and has checksum [{}]," + " size [{}]",
                            md.name(),
                            md.checksum(),
                            md.length()
                        );
                    }
                    totalSizeInBytes += md.length();
                }
                List<StoreFileMetadata> phase1Files = new ArrayList<>(diff.different.size() + diff.missing.size());
                phase1Files.addAll(diff.different);
                phase1Files.addAll(diff.missing);
                for (StoreFileMetadata md : phase1Files) {
                    if (request.metadataSnapshot().asMap().containsKey(md.name())) {
                        logger.trace(
                            "recovery [phase1]: recovering [{}], exists in local store, but is different: remote [{}], local [{}]",
                            md.name(),
                            request.metadataSnapshot().asMap().get(md.name()),
                            md
                        );
                    } else {
                        logger.trace("recovery [phase1]: recovering [{}], does not exist in remote", md.name());
                    }
                    phase1FileNames.add(md.name());
                    phase1FileSizes.add(md.length());
                    totalSizeInBytes += md.length();
                }

                logger.trace(
                    "recovery [phase1]: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]",
                    phase1FileNames.size(),
                    new ByteSizeValue(totalSizeInBytes),
                    phase1ExistingFileNames.size(),
                    new ByteSizeValue(existingTotalSizeInBytes)
                );
                final StepListener<Void> sendFileInfoStep = new StepListener<>();
                final StepListener<Void> sendFilesStep = new StepListener<>();
                final StepListener<RetentionLease> createRetentionLeaseStep = new StepListener<>();
                final StepListener<Void> cleanFilesStep = new StepListener<>();
                cancellableThreads.checkForCancel();
                recoveryTarget.receiveFileInfo(
                    phase1FileNames,
                    phase1FileSizes,
                    phase1ExistingFileNames,
                    phase1ExistingFileSizes,
                    translogOps.getAsInt(),
                    sendFileInfoStep
                );

                sendFileInfoStep.whenComplete(
                    r -> sendFiles(store, phase1Files.toArray(new StoreFileMetadata[0]), translogOps, sendFilesStep),
                    listener::onFailure
                );

                // When doing peer recovery of remote store enabled replica, retention leases are not required.
                if (skipCreateRetentionLeaseStep) {
                    sendFilesStep.whenComplete(r -> createRetentionLeaseStep.onResponse(null), listener::onFailure);
                } else {
                    sendFilesStep.whenComplete(r -> createRetentionLease(startingSeqNo, createRetentionLeaseStep), listener::onFailure);
                }

                createRetentionLeaseStep.whenComplete(retentionLease -> {
                    final long lastKnownGlobalCheckpoint = shard.getLastKnownGlobalCheckpoint();
                    assert retentionLease == null || retentionLease.retainingSequenceNumber() - 1 <= lastKnownGlobalCheckpoint
                        : retentionLease + " vs " + lastKnownGlobalCheckpoint;
                    // Establishes new empty translog on the replica with global checkpoint set to lastKnownGlobalCheckpoint. We want
                    // the commit we just copied to be a safe commit on the replica, so why not set the global checkpoint on the replica
                    // to the max seqno of this commit? Because (in rare corner cases) this commit might not be a safe commit here on
                    // the primary, and in these cases the max seqno would be too high to be valid as a global checkpoint.
                    cleanFiles(store, recoverySourceMetadata, translogOps, lastKnownGlobalCheckpoint, cleanFilesStep);
                }, listener::onFailure);

                final long totalSize = totalSizeInBytes;
                final long existingTotalSize = existingTotalSizeInBytes;
                cleanFilesStep.whenComplete(r -> {
                    final TimeValue took = stopWatch.totalTime();
                    logger.trace("recovery [phase1]: took [{}]", took);
                    listener.onResponse(
                        new SendFileResult(
                            phase1FileNames,
                            phase1FileSizes,
                            totalSize,
                            phase1ExistingFileNames,
                            phase1ExistingFileSizes,
                            existingTotalSize,
                            took
                        )
                    );
                }, listener::onFailure);
            } else {
                logger.trace("skipping [phase1] since source and target have identical sync id [{}]", recoverySourceMetadata.getSyncId());

                // but we must still create a retention lease
                final StepListener<RetentionLease> createRetentionLeaseStep = new StepListener<>();
                createRetentionLease(startingSeqNo, createRetentionLeaseStep);
                createRetentionLeaseStep.whenComplete(retentionLease -> {
                    final TimeValue took = stopWatch.totalTime();
                    logger.trace("recovery [phase1]: took [{}]", took);
                    listener.onResponse(
                        new SendFileResult(
                            Collections.emptyList(),
                            Collections.emptyList(),
                            0L,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            0L,
                            took
                        )
                    );
                }, listener::onFailure);

            }
        } catch (Exception e) {
            throw new RecoverFilesRecoveryException(request.shardId(), 0, new ByteSizeValue(0L), e);
        }
    }

    void sendFiles(Store store, StoreFileMetadata[] files, IntSupplier translogOps, ActionListener<Void> listener) {
        final MultiChunkTransfer<StoreFileMetadata, SegmentFileTransferHandler.FileChunk> transfer = transferHandler.createTransfer(
            store,
            files,
            translogOps,
            listener
        );
        resources.add(transfer);
        transfer.start();
    }

    void createRetentionLease(final long startingSeqNo, ActionListener<RetentionLease> listener) {
        RunUnderPrimaryPermit.run(() -> {
            // Clone the peer recovery retention lease belonging to the source shard. We are retaining history between the the local
            // checkpoint of the safe commit we're creating and this lease's retained seqno with the retention lock, and by cloning an
            // existing lease we (approximately) know that all our peers are also retaining history as requested by the cloned lease. If
            // the recovery now fails before copying enough history over then a subsequent attempt will find this lease, determine it is
            // not enough, and fall back to a file-based recovery.
            //
            // (approximately) because we do not guarantee to be able to satisfy every lease on every peer.
            logger.trace("cloning primary's retention lease");
            try {
                final StepListener<ReplicationResponse> cloneRetentionLeaseStep = new StepListener<>();
                final RetentionLease clonedLease = shard.cloneLocalPeerRecoveryRetentionLease(
                    request.targetNode().getId(),
                    new ThreadedActionListener<>(logger, shard.getThreadPool(), ThreadPool.Names.GENERIC, cloneRetentionLeaseStep, false)
                );
                logger.trace("cloned primary's retention lease as [{}]", clonedLease);
                cloneRetentionLeaseStep.whenComplete(rr -> listener.onResponse(clonedLease), listener::onFailure);
            } catch (RetentionLeaseNotFoundException e) {
                // it's possible that the primary has no retention lease yet if we are doing a rolling upgrade from a version before
                // 7.4, and in that case we just create a lease using the local checkpoint of the safe commit which we're using for
                // recovery as a conservative estimate for the global checkpoint.
                assert shard.indexSettings().isSoftDeleteEnabled() == false;
                final StepListener<ReplicationResponse> addRetentionLeaseStep = new StepListener<>();
                final long estimatedGlobalCheckpoint = startingSeqNo - 1;
                final RetentionLease newLease = shard.addPeerRecoveryRetentionLease(
                    request.targetNode().getId(),
                    estimatedGlobalCheckpoint,
                    new ThreadedActionListener<>(logger, shard.getThreadPool(), ThreadPool.Names.GENERIC, addRetentionLeaseStep, false)
                );
                addRetentionLeaseStep.whenComplete(rr -> listener.onResponse(newLease), listener::onFailure);
                logger.trace("created retention lease with estimated checkpoint of [{}]", estimatedGlobalCheckpoint);
            }
        }, shardId + " establishing retention lease for [" + request.targetAllocationId() + "]", shard, cancellableThreads, logger);
    }

    boolean canSkipPhase1(Store.MetadataSnapshot source, Store.MetadataSnapshot target) {
        if (source.getSyncId() == null || source.getSyncId().equals(target.getSyncId()) == false) {
            return false;
        }
        if (source.getNumDocs() != target.getNumDocs()) {
            throw new IllegalStateException(
                "try to recover "
                    + request.shardId()
                    + " from primary shard with sync id but number "
                    + "of docs differ: "
                    + source.getNumDocs()
                    + " ("
                    + request.sourceNode().getName()
                    + ", primary) vs "
                    + target.getNumDocs()
                    + "("
                    + request.targetNode().getName()
                    + ")"
            );
        }
        SequenceNumbers.CommitInfo sourceSeqNos = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(source.getCommitUserData().entrySet());
        SequenceNumbers.CommitInfo targetSeqNos = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(target.getCommitUserData().entrySet());
        if (sourceSeqNos.localCheckpoint != targetSeqNos.localCheckpoint || targetSeqNos.maxSeqNo != sourceSeqNos.maxSeqNo) {
            final String message = "try to recover "
                + request.shardId()
                + " with sync id but "
                + "seq_no stats are mismatched: ["
                + source.getCommitUserData()
                + "] vs ["
                + target.getCommitUserData()
                + "]";
            assert false : message;
            throw new IllegalStateException(message);
        }
        return true;
    }

    void prepareTargetForTranslog(int totalTranslogOps, ActionListener<TimeValue> listener) {
        StopWatch stopWatch = new StopWatch().start();
        final ActionListener<Void> wrappedListener = ActionListener.wrap(nullVal -> {
            stopWatch.stop();
            final TimeValue tookTime = stopWatch.totalTime();
            logger.trace("recovery [phase1]: remote engine start took [{}]", tookTime);
            listener.onResponse(tookTime);
        }, e -> listener.onFailure(new RecoveryEngineException(shard.shardId(), 1, "prepare target for translog failed", e)));
        // Send a request preparing the new shard's translog to receive operations. This ensures the shard engine is started and disables
        // garbage collection (not the JVM's GC!) of tombstone deletes.
        logger.trace("recovery [phase1]: prepare remote engine for translog");
        cancellableThreads.checkForCancel();
        recoveryTarget.prepareForTranslogOperations(totalTranslogOps, wrappedListener);
    }

    /**
     * Perform phase two of the recovery process.
     * <p>
     * Phase two uses a snapshot of the current translog *without* acquiring the write lock (however, the translog snapshot is
     * point-in-time view of the translog). It then sends each translog operation to the target node so it can be replayed into the new
     * shard.
     *
     * @param startingSeqNo              the sequence number to start recovery from, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO} if all
     *                                   ops should be sent
     * @param endingSeqNo                the highest sequence number that should be sent
     * @param snapshot                   a snapshot of the translog
     * @param maxSeenAutoIdTimestamp     the max auto_id_timestamp of append-only requests on the primary
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates or deletes on the primary after these operations were executed on it.
     * @param listener                   a listener which will be notified with the local checkpoint on the target.
     */
    void phase2(
        final long startingSeqNo,
        final long endingSeqNo,
        final Translog.Snapshot snapshot,
        final long maxSeenAutoIdTimestamp,
        final long maxSeqNoOfUpdatesOrDeletes,
        final RetentionLeases retentionLeases,
        final long mappingVersion,
        final ActionListener<SendSnapshotResult> listener
    ) throws IOException {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        logger.trace("recovery [phase2]: sending transaction log operations (from [" + startingSeqNo + "] to [" + endingSeqNo + "]");
        final StopWatch stopWatch = new StopWatch().start();
        final StepListener<Void> sendListener = new StepListener<>();
        final OperationBatchSender sender = new OperationBatchSender(
            startingSeqNo,
            endingSeqNo,
            snapshot,
            maxSeenAutoIdTimestamp,
            maxSeqNoOfUpdatesOrDeletes,
            retentionLeases,
            mappingVersion,
            sendListener
        );
        sendListener.whenComplete(ignored -> {
            final long skippedOps = sender.skippedOps.get();
            final int totalSentOps = sender.sentOps.get();
            final long targetLocalCheckpoint = sender.targetLocalCheckpoint.get();
            assert snapshot.totalOperations() == snapshot.skippedOperations() + skippedOps + totalSentOps : String.format(
                Locale.ROOT,
                "expected total [%d], overridden [%d], skipped [%d], total sent [%d]",
                snapshot.totalOperations(),
                snapshot.skippedOperations(),
                skippedOps,
                totalSentOps
            );
            stopWatch.stop();
            final TimeValue tookTime = stopWatch.totalTime();
            logger.trace("recovery [phase2]: took [{}]", tookTime);
            listener.onResponse(new SendSnapshotResult(targetLocalCheckpoint, totalSentOps, tookTime));
        }, listener::onFailure);
        sender.start();
    }

    /**
     * An operation chunk request
     *
     * @opensearch.internal
     */
    private static class OperationChunkRequest implements MultiChunkTransfer.ChunkRequest {
        final List<Translog.Operation> operations;
        final boolean lastChunk;

        OperationChunkRequest(List<Translog.Operation> operations, boolean lastChunk) {
            this.operations = operations;
            this.lastChunk = lastChunk;
        }

        @Override
        public boolean lastChunk() {
            return lastChunk;
        }
    }

    private class OperationBatchSender extends MultiChunkTransfer<Translog.Snapshot, OperationChunkRequest> {
        private final long startingSeqNo;
        private final long endingSeqNo;
        private final Translog.Snapshot snapshot;
        private final long maxSeenAutoIdTimestamp;
        private final long maxSeqNoOfUpdatesOrDeletes;
        private final RetentionLeases retentionLeases;
        private final long mappingVersion;
        private int lastBatchCount = 0; // used to estimate the count of the subsequent batch.
        private final AtomicInteger skippedOps = new AtomicInteger();
        private final AtomicInteger sentOps = new AtomicInteger();
        private final AtomicLong targetLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        OperationBatchSender(
            long startingSeqNo,
            long endingSeqNo,
            Translog.Snapshot snapshot,
            long maxSeenAutoIdTimestamp,
            long maxSeqNoOfUpdatesOrDeletes,
            RetentionLeases retentionLeases,
            long mappingVersion,
            ActionListener<Void> listener
        ) {
            super(logger, threadPool.getThreadContext(), listener, maxConcurrentOperations, Collections.singletonList(snapshot));
            this.startingSeqNo = startingSeqNo;
            this.endingSeqNo = endingSeqNo;
            this.snapshot = snapshot;
            this.maxSeenAutoIdTimestamp = maxSeenAutoIdTimestamp;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
            this.retentionLeases = retentionLeases;
            this.mappingVersion = mappingVersion;
        }

        @Override
        protected synchronized OperationChunkRequest nextChunkRequest(Translog.Snapshot snapshot) throws IOException {
            // We need to synchronized Snapshot#next() because it's called by different threads through sendBatch.
            // Even though those calls are not concurrent, Snapshot#next() uses non-synchronized state and is not multi-thread-compatible.
            assert Transports.assertNotTransportThread("[phase2]");
            cancellableThreads.checkForCancel();
            final List<Translog.Operation> ops = lastBatchCount > 0 ? new ArrayList<>(lastBatchCount) : new ArrayList<>();
            long batchSizeInBytes = 0L;
            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                if (shard.state() == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(request.shardId());
                }
                final long seqNo = operation.seqNo();
                if (seqNo < startingSeqNo || seqNo > endingSeqNo) {
                    skippedOps.incrementAndGet();
                    continue;
                }
                ops.add(operation);
                batchSizeInBytes += operation.estimateSize();
                sentOps.incrementAndGet();

                // check if this request is past bytes threshold, and if so, send it off
                if (batchSizeInBytes >= chunkSizeInBytes) {
                    break;
                }
            }
            lastBatchCount = ops.size();
            return new OperationChunkRequest(ops, operation == null);
        }

        @Override
        protected void executeChunkRequest(OperationChunkRequest request, ActionListener<Void> listener) {
            cancellableThreads.checkForCancel();
            recoveryTarget.indexTranslogOperations(
                request.operations,
                snapshot.totalOperations(),
                maxSeenAutoIdTimestamp,
                maxSeqNoOfUpdatesOrDeletes,
                retentionLeases,
                mappingVersion,
                ActionListener.delegateFailure(listener, (l, newCheckpoint) -> {
                    targetLocalCheckpoint.updateAndGet(curr -> SequenceNumbers.max(curr, newCheckpoint));
                    l.onResponse(null);
                })
            );
        }

        @Override
        protected void handleError(Translog.Snapshot snapshot, Exception e) {
            throw new RecoveryEngineException(shard.shardId(), 2, "failed to send/replay operations", e);
        }

        @Override
        public void close() throws IOException {
            snapshot.close();
        }
    }

    void finalizeRecovery(long targetLocalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) throws IOException {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("finalizing recovery");
        /*
         * Before marking the shard as in-sync we acquire an operation permit. We do this so that there is a barrier between marking a
         * shard as in-sync and relocating a shard. If we acquire the permit then no relocation handoff can complete before we are done
         * marking the shard as in-sync. If the relocation handoff holds all the permits then after the handoff completes and we acquire
         * the permit then the state of the shard will be relocated and this recovery will fail.
         */
        RunUnderPrimaryPermit.run(
            () -> shard.markAllocationIdAsInSync(request.targetAllocationId(), targetLocalCheckpoint),
            shardId + " marking " + request.targetAllocationId() + " as in sync",
            shard,
            cancellableThreads,
            logger
        );
        final long globalCheckpoint = shard.getLastKnownGlobalCheckpoint(); // this global checkpoint is persisted in finalizeRecovery
        final StepListener<Void> finalizeListener = new StepListener<>();
        cancellableThreads.checkForCancel();
        recoveryTarget.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, finalizeListener);
        finalizeListener.whenComplete(r -> {
            RunUnderPrimaryPermit.run(
                () -> shard.updateGlobalCheckpointForShard(request.targetAllocationId(), globalCheckpoint),
                shardId + " updating " + request.targetAllocationId() + "'s global checkpoint",
                shard,
                cancellableThreads,
                logger
            );

            if (request.isPrimaryRelocation()) {
                logger.trace("performing relocation hand-off");
                final Runnable forceSegRepRunnable = shard.indexSettings().isSegRepEnabled()
                    ? recoveryTarget::forceSegmentFileSync
                    : () -> {};
                // TODO: make relocated async
                // this acquires all IndexShard operation permits and will thus delay new recoveries until it is done
                cancellableThreads.execute(
                    () -> shard.relocated(request.targetAllocationId(), recoveryTarget::handoffPrimaryContext, forceSegRepRunnable)
                );
                /*
                 * if the recovery process fails after disabling primary mode on the source shard, both relocation source and
                 * target are failed (see {@link IndexShard#updateRoutingEntry}).
                 */
            } else {
                // Force round of segment replication to update its checkpoint to primary's
                if (shard.indexSettings().isSegRepEnabled()) {
                    cancellableThreads.execute(recoveryTarget::forceSegmentFileSync);
                }
            }
            stopWatch.stop();
            logger.info("finalizing recovery took [{}]", stopWatch.totalTime());
            listener.onResponse(null);
        }, listener::onFailure);
    }

    /**
     * A result for a send snapshot
     *
     * @opensearch.internal
     */
    static final class SendSnapshotResult {
        final long targetLocalCheckpoint;
        final int sentOperations;
        final TimeValue tookTime;

        SendSnapshotResult(final long targetLocalCheckpoint, final int sentOperations, final TimeValue tookTime) {
            this.targetLocalCheckpoint = targetLocalCheckpoint;
            this.sentOperations = sentOperations;
            this.tookTime = tookTime;
        }
    }

    /**
     * Cancels the recovery and interrupts all eligible threads.
     */
    public void cancel(String reason) {
        cancellableThreads.cancel(reason);
        recoveryTarget.cancel();
    }

    @Override
    public String toString() {
        return "ShardRecoveryHandler{"
            + "shardId="
            + request.shardId()
            + ", sourceNode="
            + request.sourceNode()
            + ", targetNode="
            + request.targetNode()
            + '}';
    }

    private void cleanFiles(
        Store store,
        Store.MetadataSnapshot sourceMetadata,
        IntSupplier translogOps,
        long globalCheckpoint,
        ActionListener<Void> listener
    ) {
        // Send the CLEAN_FILES request, which takes all of the files that
        // were transferred and renames them from their temporary file
        // names to the actual file names. It also writes checksums for
        // the files after they have been renamed.
        //
        // Once the files have been renamed, any other files that are not
        // related to this recovery (out of date segments, for example)
        // are deleted
        cancellableThreads.checkForCancel();
        recoveryTarget.cleanFiles(
            translogOps.getAsInt(),
            globalCheckpoint,
            sourceMetadata,
            ActionListener.delegateResponse(listener, (l, e) -> ActionListener.completeWith(l, () -> {
                StoreFileMetadata[] mds = StreamSupport.stream(sourceMetadata.spliterator(), false).toArray(StoreFileMetadata[]::new);
                ArrayUtil.timSort(mds, Comparator.comparingLong(StoreFileMetadata::length)); // check small files first
                transferHandler.handleErrorOnSendFiles(store, e, mds);
                throw e;
            }))
        );
    }
}
