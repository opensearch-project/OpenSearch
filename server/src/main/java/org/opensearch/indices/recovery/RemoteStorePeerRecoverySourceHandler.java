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
import org.opensearch.common.SetOnce;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.RecoveryEngineException;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngineCatalogSnapshot;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.RunUnderPrimaryPermit;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transports;

import java.io.IOException;
import java.util.Map;
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
        waitForAssignmentPropagate(retentionLeaseRef);
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

        final long startingSeqNo = resolveStartingSeqNo(wrappedSafeCommit.get());
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
        assert startingSeqNo >= 0 || startingSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO
            : "startingSeqNo must be non negative or UNASSIGNED_SEQ_NO. got: " + startingSeqNo;

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

    /**
     * Resolves the starting sequence number from the commit's user data.
     * For optimized indices, the {@link SequenceNumbers#LOCAL_CHECKPOINT_KEY} may not be present
     * in the Lucene commit user data. In that case, we attempt to extract it from the
     * {@link CatalogSnapshot} user data (which is serialized within the commit). If neither
     * source contains the key, we fall back to {@link SequenceNumbers#UNASSIGNED_SEQ_NO}.
     *
     * @param indexCommit the safe index commit to extract the starting sequence number from
     * @return the starting sequence number (local checkpoint + 1), or {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
     * @throws IOException if there is an error reading commit user data or deserializing the CatalogSnapshot
     */
    private long resolveStartingSeqNo(IndexCommit indexCommit) throws IOException {
        final Map<String, String> commitUserData = indexCommit.getUserData();
        final String localCheckpointValue = commitUserData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY);

        if (localCheckpointValue != null) {
            return Long.parseLong(localCheckpointValue) + 1L;
        }

        // For optimized indices, try to extract from CatalogSnapshot user data
        if (shard.isOptimizedIndex()) {
            final String serializedCatalogSnapshot = commitUserData.get(CompositeEngineCatalogSnapshot.CATALOG_SNAPSHOT_KEY);
            if (serializedCatalogSnapshot != null) {
                final CatalogSnapshot catalogSnapshot = CompositeEngineCatalogSnapshot.deserializeFromString(serializedCatalogSnapshot);
                final Map<String, String> catalogUserData = catalogSnapshot.getUserData();
                if (catalogUserData != null) {
                    final String catalogCheckpointValue = catalogUserData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY);
                    if (catalogCheckpointValue != null) {
                        logger.debug(
                            "LOCAL_CHECKPOINT_KEY not found in commit user data for optimized index, "
                                + "extracted from CatalogSnapshot user data: [{}]",
                            catalogCheckpointValue
                        );
                        return Long.parseLong(catalogCheckpointValue) + 1L;
                    }
                }
            }
            logger.warn(
                "LOCAL_CHECKPOINT_KEY not found in commit or CatalogSnapshot user data for optimized index [{}], "
                    + "using UNASSIGNED_SEQ_NO as safe default",
                shard.shardId()
            );
            return SequenceNumbers.UNASSIGNED_SEQ_NO;
        }

        // For non-optimized indices, this should not happen - throw to preserve existing behavior
        throw new IllegalStateException(
            "LOCAL_CHECKPOINT_KEY not found in commit user data for shard [" + shard.shardId() + "]"
        );
    }

}
