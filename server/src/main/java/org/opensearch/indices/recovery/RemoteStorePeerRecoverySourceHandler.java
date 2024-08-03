/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.StepListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.SetOnce;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.RecoveryEngineException;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.RunUnderPrimaryPermit;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transports;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * This handler is used when peer recovery target is a remote store enabled replica.
 *
 * @opensearch.internal
 */
public class RemoteStorePeerRecoverySourceHandler extends RecoverySourceHandler {

    public RemoteStorePeerRecoverySourceHandler(
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
        // A replica of an index with remote translog does not require the translogs locally and keeps receiving the
        // updated segments file on refresh, flushes, and merges. In recovery, here, only file-based recovery is performed
        // and there is no translog replay done.
        final SetOnce<RetentionLease> retentionLeaseRef = new SetOnce<>();
        waitForAssignment(retentionLeaseRef);
        final StepListener<SendFileResult> sendFileStep = new StepListener<>();
        final StepListener<TimeValue> prepareEngineStep = new StepListener<>();
        final StepListener<SendSnapshotResult> sendSnapshotStep = new StepListener<>();

        // It is always file based recovery while recovering replicas which are not relocating primary where the
        // underlying indices are backed by remote store for storing segments and translog

        final GatedCloseable<IndexCommit> wrappedSafeCommit;
        try {
            wrappedSafeCommit = acquireSafeCommit(shard);
            resources.add(wrappedSafeCommit);
        } catch (final Exception e) {
            throw new RecoveryEngineException(shard.shardId(), 1, "snapshot failed", e);
        }

        final long startingSeqNo = Long.parseLong(wrappedSafeCommit.get().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1L;
        logger.trace("performing file-based recovery followed by history replay starting at [{}]", startingSeqNo);

        try {
            final Releasable releaseStore = acquireStore(shard.store());
            resources.add(releaseStore);
            onSendFileStepComplete(sendFileStep, wrappedSafeCommit, releaseStore);

            assert Transports.assertNotTransportThread(this + "[phase1]");
            phase1(wrappedSafeCommit.get(), startingSeqNo, () -> 0, sendFileStep, true);
        } catch (final Exception e) {
            throw new RecoveryEngineException(shard.shardId(), 1, "sendFileStep failed", e);
        }
        assert startingSeqNo >= 0 : "startingSeqNo must be non negative. got: " + startingSeqNo;

        sendFileStep.whenComplete(r -> {
            logger.debug("sendFileStep completed");
            assert Transports.assertNotTransportThread(this + "[prepareTargetForTranslog]");
            // For a sequence based recovery, the target can keep its local translog
            prepareTargetForTranslog(0, prepareEngineStep);
        }, onFailure);

        prepareEngineStep.whenComplete(prepareEngineTime -> {
            logger.debug("prepareEngineStep completed");
            assert Transports.assertNotTransportThread(this + "[phase2]");
            RunUnderPrimaryPermit.run(
                () -> shard.initiateTracking(request.targetAllocationId()),
                shardId + " initiating tracking of " + request.targetAllocationId(),
                shard,
                cancellableThreads,
                logger
            );
            final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
            sendSnapshotStep.onResponse(new SendSnapshotResult(endingSeqNo, 0, TimeValue.ZERO));
        }, onFailure);

        finalizeStepAndCompleteFuture(startingSeqNo, sendSnapshotStep, sendFileStep, prepareEngineStep, onFailure);
    }

    protected void waitForAssignment(SetOnce<RetentionLease> retentionLeaseRef)  {
        BackoffPolicy EXPONENTIAL_BACKOFF_POLICY = BackoffPolicy.exponentialBackoff(
            TimeValue.timeValueMillis(100),
            3
        );
        AtomicReference<ShardRouting> targetShardRouting = new AtomicReference<>();
        Iterator<TimeValue>  backoffDelayIterator = EXPONENTIAL_BACKOFF_POLICY.iterator();
        while (backoffDelayIterator.hasNext() ) {
            RunUnderPrimaryPermit.run(() -> {
                final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
                targetShardRouting.set(routingTable.getByAllocationId(request.targetAllocationId()));
                if (targetShardRouting.get() == null) {
                    logger.info(
                        "delaying recovery of {} as it is not listed as assigned to target node {}",
                        request.shardId(),
                        request.targetNode()
                    );
                    Thread.sleep(backoffDelayIterator.next().millis());
                }
                if (targetShardRouting.get() != null) {
                    assert targetShardRouting.get().initializing() : "expected recovery target to be initializing but was " + targetShardRouting;
                    retentionLeaseRef.set(shard.getRetentionLeases().get(ReplicationTracker.getPeerRecoveryRetentionLeaseId(targetShardRouting.get())));
                }

            }, shardId + " validating recovery target [" + request.targetAllocationId() + "] registered ", shard, cancellableThreads, logger);

            if (targetShardRouting.get() != null) {
                return;
            }
        }
        if (targetShardRouting.get() != null) {
            return;
        }
        throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
    }

}
