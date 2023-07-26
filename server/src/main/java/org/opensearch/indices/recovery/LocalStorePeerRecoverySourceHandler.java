/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.SetOnce;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.lease.Releasable;
import org.opensearch.index.engine.RecoveryEngineException;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.RetentionLeaseNotFoundException;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.RunUnderPrimaryPermit;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * This handler is used for node-to-node peer recovery when the recovery target is a replica/ or a relocating primary
 * shard with translog backed by local store.
 *
 * @opensearch.internal
 */
public class LocalStorePeerRecoverySourceHandler extends RecoverySourceHandler {

    public LocalStorePeerRecoverySourceHandler(
        IndexShard shard,
        RecoveryTargetHandler recoveryTarget,
        ThreadPool threadPool,
        StartRecoveryRequest request,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks,
        int maxConcurrentOperations
    ) {
        super(shard, recoveryTarget, threadPool, request, fileChunkSizeInBytes, maxConcurrentFileChunks, maxConcurrentOperations);
    }

    @Override
    protected void innerRecoveryToTarget(ActionListener<RecoveryResponse> listener, Consumer<Exception> onFailure) throws IOException {
        final SetOnce<RetentionLease> retentionLeaseRef = new SetOnce<>();

        RunUnderPrimaryPermit.run(() -> {
            final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
            ShardRouting targetShardRouting = routingTable.getByAllocationId(request.targetAllocationId());
            if (targetShardRouting == null) {
                logger.debug(
                    "delaying recovery of {} as it is not listed as assigned to target node {}",
                    request.shardId(),
                    request.targetNode()
                );
                throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
            }
            assert targetShardRouting.initializing() : "expected recovery target to be initializing but was " + targetShardRouting;
            retentionLeaseRef.set(shard.getRetentionLeases().get(ReplicationTracker.getPeerRecoveryRetentionLeaseId(targetShardRouting)));
        }, shardId + " validating recovery target [" + request.targetAllocationId() + "] registered ", shard, cancellableThreads, logger);
        final Closeable retentionLock = shard.acquireHistoryRetentionLock();
        resources.add(retentionLock);
        final long startingSeqNo;
        final boolean isSequenceNumberBasedRecovery = request.startingSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
            && isTargetSameHistory()
            && shard.hasCompleteHistoryOperations(PEER_RECOVERY_NAME, request.startingSeqNo())
            && ((retentionLeaseRef.get() == null && shard.useRetentionLeasesInPeerRecovery() == false)
                || (retentionLeaseRef.get() != null && retentionLeaseRef.get().retainingSequenceNumber() <= request.startingSeqNo()));
        // NB check hasCompleteHistoryOperations when computing isSequenceNumberBasedRecovery, even if there is a retention lease,
        // because when doing a rolling upgrade from earlier than 7.4 we may create some leases that are initially unsatisfied. It's
        // possible there are other cases where we cannot satisfy all leases, because that's not a property we currently expect to hold.
        // Also it's pretty cheap when soft deletes are enabled, and it'd be a disaster if we tried a sequence-number-based recovery
        // without having a complete history.

        if (isSequenceNumberBasedRecovery && retentionLeaseRef.get() != null) {
            // all the history we need is retained by an existing retention lease, so we do not need a separate retention lock
            retentionLock.close();
            logger.trace("history is retained by {}", retentionLeaseRef.get());
        } else {
            // all the history we need is retained by the retention lock, obtained before calling shard.hasCompleteHistoryOperations()
            // and before acquiring the safe commit we'll be using, so we can be certain that all operations after the safe commit's
            // local checkpoint will be retained for the duration of this recovery.
            logger.trace("history is retained by retention lock");
        }

        final StepListener<SendFileResult> sendFileStep = new StepListener<>();
        final StepListener<TimeValue> prepareEngineStep = new StepListener<>();
        final StepListener<SendSnapshotResult> sendSnapshotStep = new StepListener<>();

        if (isSequenceNumberBasedRecovery) {
            logger.trace("performing sequence numbers based recovery. starting at [{}]", request.startingSeqNo());
            startingSeqNo = request.startingSeqNo();
            if (retentionLeaseRef.get() == null) {
                createRetentionLease(startingSeqNo, ActionListener.map(sendFileStep, ignored -> SendFileResult.EMPTY));
            } else {
                sendFileStep.onResponse(SendFileResult.EMPTY);
            }
        } else {
            final GatedCloseable<IndexCommit> wrappedSafeCommit;
            try {
                wrappedSafeCommit = acquireSafeCommit(shard);
                resources.add(wrappedSafeCommit);
            } catch (final Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "snapshot failed", e);
            }

            // Try and copy enough operations to the recovering peer so that if it is promoted to primary then it has a chance of being
            // able to recover other replicas using operations-based recoveries. If we are not using retention leases then we
            // conservatively copy all available operations. If we are using retention leases then "enough operations" is just the
            // operations from the local checkpoint of the safe commit onwards, because when using soft deletes the safe commit retains
            // at least as much history as anything else. The safe commit will often contain all the history retained by the current set
            // of retention leases, but this is not guaranteed: an earlier peer recovery from a different primary might have created a
            // retention lease for some history that this primary already discarded, since we discard history when the global checkpoint
            // advances and not when creating a new safe commit. In any case this is a best-effort thing since future recoveries can
            // always fall back to file-based ones, and only really presents a problem if this primary fails before things have settled
            // down.
            startingSeqNo = Long.parseLong(wrappedSafeCommit.get().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1L;
            logger.trace("performing file-based recovery followed by history replay starting at [{}]", startingSeqNo);

            try {
                final int estimateNumOps = countNumberOfHistoryOperations(startingSeqNo);
                final Releasable releaseStore = acquireStore(shard.store());
                resources.add(releaseStore);
                onSendFileStepComplete(sendFileStep, wrappedSafeCommit, releaseStore);

                final StepListener<ReplicationResponse> deleteRetentionLeaseStep = new StepListener<>();
                RunUnderPrimaryPermit.run(() -> {
                    try {
                        // If the target previously had a copy of this shard then a file-based recovery might move its global
                        // checkpoint backwards. We must therefore remove any existing retention lease so that we can create a
                        // new one later on in the recovery.
                        shard.removePeerRecoveryRetentionLease(
                            request.targetNode().getId(),
                            new ThreadedActionListener<>(
                                logger,
                                shard.getThreadPool(),
                                ThreadPool.Names.GENERIC,
                                deleteRetentionLeaseStep,
                                false
                            )
                        );
                    } catch (RetentionLeaseNotFoundException e) {
                        logger.debug("no peer-recovery retention lease for " + request.targetAllocationId());
                        deleteRetentionLeaseStep.onResponse(null);
                    }
                }, shardId + " removing retention lease for [" + request.targetAllocationId() + "]", shard, cancellableThreads, logger);

                deleteRetentionLeaseStep.whenComplete(ignored -> {
                    assert Transports.assertNotTransportThread(this + "[phase1]");
                    phase1(wrappedSafeCommit.get(), startingSeqNo, () -> estimateNumOps, sendFileStep, false);
                }, onFailure);

            } catch (final Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "sendFileStep failed", e);
            }
        }
        assert startingSeqNo >= 0 : "startingSeqNo must be non negative. got: " + startingSeqNo;

        sendFileStep.whenComplete(r -> {
            assert Transports.assertNotTransportThread(this + "[prepareTargetForTranslog]");
            // For a sequence based recovery, the target can keep its local translog
            prepareTargetForTranslog(countNumberOfHistoryOperations(startingSeqNo), prepareEngineStep);
        }, onFailure);

        prepareEngineStep.whenComplete(prepareEngineTime -> {
            assert Transports.assertNotTransportThread(this + "[phase2]");
            /*
             * add shard to replication group (shard will receive replication requests from this point on) now that engine is open.
             * This means that any document indexed into the primary after this will be replicated to this replica as well
             * make sure to do this before sampling the max sequence number in the next step, to ensure that we send
             * all documents up to maxSeqNo in phase2.
             */
            RunUnderPrimaryPermit.run(
                () -> shard.initiateTracking(request.targetAllocationId()),
                shardId + " initiating tracking of " + request.targetAllocationId(),
                shard,
                cancellableThreads,
                logger
            );

            final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
            if (logger.isTraceEnabled()) {
                logger.trace("snapshot translog for recovery; current size is [{}]", countNumberOfHistoryOperations(startingSeqNo));
            }
            final Translog.Snapshot phase2Snapshot = shard.newChangesSnapshot(
                PEER_RECOVERY_NAME,
                startingSeqNo,
                Long.MAX_VALUE,
                false,
                true
            );
            resources.add(phase2Snapshot);
            retentionLock.close();

            // we have to capture the max_seen_auto_id_timestamp and the max_seq_no_of_updates to make sure that these values
            // are at least as high as the corresponding values on the primary when any of these operations were executed on it.
            final long maxSeenAutoIdTimestamp = shard.getMaxSeenAutoIdTimestamp();
            final long maxSeqNoOfUpdatesOrDeletes = shard.getMaxSeqNoOfUpdatesOrDeletes();
            final RetentionLeases retentionLeases = shard.getRetentionLeases();
            final long mappingVersionOnPrimary = shard.indexSettings().getIndexMetadata().getMappingVersion();
            phase2(
                startingSeqNo,
                endingSeqNo,
                phase2Snapshot,
                maxSeenAutoIdTimestamp,
                maxSeqNoOfUpdatesOrDeletes,
                retentionLeases,
                mappingVersionOnPrimary,
                sendSnapshotStep
            );

        }, onFailure);
        finalizeStepAndCompleteFuture(startingSeqNo, sendSnapshotStep, sendFileStep, prepareEngineStep, onFailure);
    }
}
