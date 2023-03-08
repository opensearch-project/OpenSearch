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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.opensearch.Assertions;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MapperException;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardNotRecoveringException;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationTarget;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.index.translog.Translog.TRANSLOG_UUID_KEY;

/**
 * Represents a recovery where the current node is the target node of the recovery. To track recoveries in a central place, instances of
 * this class are created through {@link ReplicationCollection}.
 *
 * @opensearch.internal
 */
public class RecoveryTarget extends ReplicationTarget implements RecoveryTargetHandler {

    private static final String RECOVERY_PREFIX = "recovery.";

    private final DiscoveryNode sourceNode;
    protected final MultiFileWriter multiFileWriter;

    // latch that can be used to blockingly wait for RecoveryTarget to be closed
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard                        local shard where we want to recover to
     * @param sourceNode                        source node of the recovery where we recover from
     * @param listener                          called when recovery is completed/failed
     */
    public RecoveryTarget(IndexShard indexShard, DiscoveryNode sourceNode, ReplicationListener listener) {
        super("recovery_status", indexShard, indexShard.recoveryState().getIndex(), listener);
        this.sourceNode = sourceNode;
        indexShard.recoveryStats().incCurrentAsTarget();
        final String tempFilePrefix = getPrefix() + UUIDs.randomBase64UUID() + ".";
        this.multiFileWriter = new MultiFileWriter(indexShard.store(), stateIndex, tempFilePrefix, logger, this::ensureRefCount);
    }

    /**
     * Returns a fresh recovery target to retry recovery from the same source node onto the same shard and using the same listener.
     *
     * @return a copy of this recovery target
     */
    public RecoveryTarget retryCopy() {
        return new RecoveryTarget(indexShard, sourceNode, listener);
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    public String source() {
        return sourceNode.toString();
    }

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public RecoveryState state() {
        return indexShard.recoveryState();
    }

    public CancellableThreads cancellableThreads() {
        return cancellableThreads;
    }

    public String description() {
        return "recovery from " + source();
    }

    @Override
    public void notifyListener(ReplicationFailedException e, boolean sendShardFailure) {
        listener.onFailure(state(), new RecoveryFailedException(state(), e.getMessage(), e), sendShardFailure);
    }

    /**
     * Closes the current recovery target and waits up to a certain timeout for resources to be freed.
     * Returns true if resetting the recovery was successful, false if the recovery target is already cancelled / failed or marked as done.
     */
    public boolean reset(CancellableThreads newTargetCancellableThreads) throws IOException {
        final long recoveryId = getId();
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("reset of recovery with shard {} and id [{}]", shardId(), recoveryId);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now.
                decRef();
            }
            try {
                newTargetCancellableThreads.execute(closedLatch::await);
            } catch (CancellableThreads.ExecutionCancelledException e) {
                logger.trace(
                    "new recovery target cancelled for shard {} while waiting on old recovery target with id [{}] to close",
                    shardId(),
                    recoveryId
                );
                return false;
            }
            RecoveryState.Stage stage = indexShard.recoveryState().getStage();
            if (indexShard.recoveryState().getPrimary() && (stage == RecoveryState.Stage.FINALIZE || stage == RecoveryState.Stage.DONE)) {
                // once primary relocation has moved past the finalization step, the relocation source can put the target into primary mode
                // and start indexing as primary into the target shard (see TransportReplicationAction). Resetting the target shard in this
                // state could mean that indexing is halted until the recovery retry attempt is completed and could also destroy existing
                // documents indexed and acknowledged before the reset.
                assert stage != RecoveryState.Stage.DONE : "recovery should not have completed when it's being reset";
                throw new IllegalStateException("cannot reset recovery as previous attempt made it past finalization step");
            }
            indexShard.performRecoveryRestart();
            return true;
        }
        return false;
    }

    @Override
    protected void closeInternal() {
        try {
            multiFileWriter.close();
        } finally {
            store.decRef();
            indexShard.recoveryStats().decCurrentAsTarget();
            closedLatch.countDown();
        }
    }

    @Override
    public String toString() {
        return shardId() + " [" + getId() + "]";
    }

    @Override
    protected String getPrefix() {
        return RECOVERY_PREFIX;
    }

    @Override
    protected void onDone() {
        assert multiFileWriter.tempFileNames.isEmpty() : "not all temporary files are renamed";
        indexShard.postRecovery("peer recovery done");
    }

    /*** Implementation of {@link RecoveryTargetHandler } */

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            state().getIndex().setFileDetailsComplete(); // ops-based recoveries don't send the file details
            state().getTranslog().totalOperations(totalTranslogOps);
            indexShard().openEngineAndSkipTranslogRecovery();
            return null;
        });
    }

    @Override
    public void forceSegmentFileSync() {
        throw new UnsupportedOperationException("Method not supported on target!");
    }

    @Override
    public void finalizeRecovery(final long globalCheckpoint, final long trimAboveSeqNo, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            indexShard.updateGlobalCheckpointOnReplica(globalCheckpoint, "finalizing recovery");
            // Persist the global checkpoint.
            indexShard.sync();
            indexShard.persistRetentionLeases();
            if (trimAboveSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                // We should erase all translog operations above trimAboveSeqNo as we have received either the same or a newer copy
                // from the recovery source in phase2. Rolling a new translog generation is not strictly required here for we won't
                // trim the current generation. It's merely to satisfy the assumption that the current generation does not have any
                // operation that would be trimmed (see TranslogWriter#assertNoSeqAbove). This assumption does not hold for peer
                // recovery because we could have received operations above startingSeqNo from the previous primary terms.
                indexShard.rollTranslogGeneration();
                // the flush or translog generation threshold can be reached after we roll a new translog
                indexShard.afterWriteOperation();
                indexShard.trimOperationOfPreviousPrimaryTerms(trimAboveSeqNo);
            }
            if (hasUncommittedOperations()) {
                indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            }
            indexShard.finalizeRecovery();
            return null;
        });
    }

    private boolean hasUncommittedOperations() throws IOException {
        long localCheckpointOfCommit = Long.parseLong(indexShard.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        return indexShard.countNumberOfHistoryOperations(
            RecoverySourceHandler.PEER_RECOVERY_NAME,
            localCheckpointOfCommit + 1,
            Long.MAX_VALUE
        ) > 0;
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        indexShard.activateWithPrimaryContext(primaryContext);
    }

    @Override
    public void indexTranslogOperations(
        final List<Translog.Operation> operations,
        final int totalTranslogOps,
        final long maxSeenAutoIdTimestampOnPrimary,
        final long maxSeqNoOfDeletesOrUpdatesOnPrimary,
        final RetentionLeases retentionLeases,
        final long mappingVersionOnPrimary,
        final ActionListener<Long> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            final RecoveryState.Translog translog = state().getTranslog();
            translog.totalOperations(totalTranslogOps);
            assert indexShard().recoveryState() == state();
            if (indexShard().state() != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId(), indexShard().state());
            }
            /*
             * The maxSeenAutoIdTimestampOnPrimary received from the primary is at least the highest auto_id_timestamp from any operation
             * will be replayed. Bootstrapping this timestamp here will disable the optimization for original append-only requests
             * (source of these operations) replicated via replication. Without this step, we may have duplicate documents if we
             * replay these operations first (without timestamp), then optimize append-only requests (with timestamp).
             */
            indexShard().updateMaxUnsafeAutoIdTimestamp(maxSeenAutoIdTimestampOnPrimary);
            /*
             * Bootstrap the max_seq_no_of_updates from the primary to make sure that the max_seq_no_of_updates on this replica when
             * replaying any of these operations will be at least the max_seq_no_of_updates on the primary when that op was executed on.
             */
            indexShard().advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfDeletesOrUpdatesOnPrimary);
            /*
             * We have to update the retention leases before we start applying translog operations to ensure we are retaining according to
             * the policy.
             */
            indexShard().updateRetentionLeasesOnReplica(retentionLeases);
            for (Translog.Operation operation : operations) {
                Engine.Result result = indexShard().applyTranslogOperation(operation, Engine.Operation.Origin.PEER_RECOVERY);
                if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                    throw new MapperException("mapping updates are not allowed [" + operation + "]");
                }
                if (result.getFailure() != null) {
                    if (Assertions.ENABLED && result.getFailure() instanceof MapperException == false) {
                        throw new AssertionError("unexpected failure while replicating translog entry", result.getFailure());
                    }
                    ExceptionsHelper.reThrowIfNotNull(result.getFailure());
                }
            }
            // update stats only after all operations completed (to ensure that mapping updates don't mess with stats)
            translog.incrementRecoveredOperations(operations.size());
            indexShard().sync();
            // roll over / flush / trim if needed
            indexShard().afterWriteOperation();
            return indexShard().getLocalCheckpoint();
        });
    }

    @Override
    public void receiveFileInfo(
        List<String> phase1FileNames,
        List<Long> phase1FileSizes,
        List<String> phase1ExistingFileNames,
        List<Long> phase1ExistingFileSizes,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            indexShard.resetRecoveryStage();
            indexShard.prepareForIndexRecovery();
            final ReplicationLuceneIndex index = state().getIndex();
            for (int i = 0; i < phase1ExistingFileNames.size(); i++) {
                index.addFileDetail(phase1ExistingFileNames.get(i), phase1ExistingFileSizes.get(i), true);
            }
            for (int i = 0; i < phase1FileNames.size(); i++) {
                index.addFileDetail(phase1FileNames.get(i), phase1FileSizes.get(i), false);
            }
            index.setFileDetailsComplete();
            state().getTranslog().totalOperations(totalTranslogOps);
            state().getTranslog().totalOperationsOnStart(totalTranslogOps);
            return null;
        });
    }

    @Override
    public void cleanFiles(
        int totalTranslogOps,
        long globalCheckpoint,
        Store.MetadataSnapshot sourceMetadata,
        ActionListener<Void> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            state().getTranslog().totalOperations(totalTranslogOps);
            // first, we go and move files that were created with the recovery id suffix to
            // the actual names, its ok if we have a corrupted index here, since we have replicas
            // to recover from in case of a full cluster shutdown just when this code executes...
            multiFileWriter.renameAllTempFiles();
            final Store store = store();
            store.incRef();
            try {
                store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetadata);

                // Replicas for segment replication or remote snapshot indices do not create
                // their own commit points and therefore do not modify the commit user data
                // in their store. In these cases, reuse the primary's translog UUID.
                final boolean reuseTranslogUUID = indexShard.indexSettings().isSegRepEnabled()
                    || indexShard.indexSettings().isRemoteSnapshot();
                if (reuseTranslogUUID) {
                    final String translogUUID = store.getMetadata().getCommitUserData().get(TRANSLOG_UUID_KEY);
                    Translog.createEmptyTranslog(
                        indexShard.shardPath().resolveTranslog(),
                        shardId(),
                        globalCheckpoint,
                        indexShard.getPendingPrimaryTerm(),
                        translogUUID,
                        FileChannel::open
                    );
                } else {
                    final String translogUUID = Translog.createEmptyTranslog(
                        indexShard.shardPath().resolveTranslog(),
                        globalCheckpoint,
                        shardId(),
                        indexShard.getPendingPrimaryTerm()
                    );
                    store.associateIndexWithNewTranslog(translogUUID);
                }

                if (indexShard.getRetentionLeases().leases().isEmpty()) {
                    // if empty, may be a fresh IndexShard, so write an empty leases file to disk
                    indexShard.persistRetentionLeases();
                    assert indexShard.loadRetentionLeases().leases().isEmpty();
                } else {
                    assert indexShard.assertRetentionLeasesPersisted();
                }
                indexShard.maybeCheckIndex();
                state().setStage(RecoveryState.Stage.TRANSLOG);
            } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
                // this is a fatal exception at this stage.
                // this means we transferred files from the remote that have not be checksummed and they are
                // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
                // source shard since this index might be broken there as well? The Source can handle this and checks
                // its content on disk if possible.
                try {
                    try {
                        store.removeCorruptionMarker();
                    } finally {
                        Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                    }
                } catch (Exception e) {
                    logger.debug("Failed to clean lucene index", e);
                    ex.addSuppressed(e);
                }
                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } catch (Exception ex) {
                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } finally {
                store.decRef();
            }
            return null;
        });
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        try {
            state().getTranslog().totalOperations(totalTranslogOps);
            multiFileWriter.writeFileChunk(fileMetadata, position, content, lastChunk);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /** Get a temporary name for the provided file name. */
    public String getTempNameForFile(String origFile) {
        return multiFileWriter.getTempNameForFile(origFile);
    }

    Path translogLocation() {
        return indexShard().shardPath().resolveTranslog();
    }
}
