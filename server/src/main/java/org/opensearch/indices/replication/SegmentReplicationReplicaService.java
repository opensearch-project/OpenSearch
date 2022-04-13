/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.RetryableAction;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.breaker.CircuitBreakingException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardRecoveryException;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.DelayRecoveryException;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.ReplicationCollection;
import org.opensearch.indices.recovery.ReplicationCollection.ReplicationRef;
import org.opensearch.indices.recovery.Timer;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.copy.PrimaryShardReplicationSource;
import org.opensearch.indices.replication.copy.ReplicationCheckpoint;
import org.opensearch.indices.replication.copy.ReplicationFailedException;
import org.opensearch.indices.replication.copy.SegmentReplicationPrimaryService;
import org.opensearch.indices.replication.copy.SegmentReplicationState;
import org.opensearch.indices.replication.copy.SegmentReplicationTarget;
import org.opensearch.indices.replication.copy.TrackShardRequest;
import org.opensearch.indices.replication.copy.TrackShardResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.SendRequestTransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Orchestrator of replication events.
 */
public class SegmentReplicationReplicaService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationReplicaService.class);

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;
    private final TransportService transportService;
    private final ReplicationCollection<SegmentReplicationTarget> onGoingReplications;

    public SegmentReplicationReplicaService(
        final ThreadPool threadPool,
        final RecoverySettings recoverySettings,
        final TransportService transportService) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.transportService = transportService;
        this.onGoingReplications = new ReplicationCollection<>(logger, threadPool);
    }

    public ReplicationCollection<SegmentReplicationTarget> getOnGoingReplications() {
        return onGoingReplications;
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            onGoingReplications.cancelRecoveriesForShard(shardId, "shard closed");
        }
    }

    public void prepareForReplication(
        IndexShard indexShard,
        DiscoveryNode targetNode,
        DiscoveryNode sourceNode,
        ActionListener<TrackShardResponse> listener
    ) {
        setupReplicaShard(indexShard);
        final TimeValue initialDelay = TimeValue.timeValueMillis(200);
        final TimeValue timeout = recoverySettings.internalActionRetryTimeout();
        final RetryableAction retryableAction = new RetryableAction(logger, threadPool, initialDelay, timeout, listener) {
            @Override
            public void tryAction(ActionListener listener) {
                transportService.sendRequest(
                    sourceNode,
                    SegmentReplicationPrimaryService.Actions.TRACK_SHARD,
                    new TrackShardRequest(indexShard.shardId(), indexShard.routingEntry().allocationId().getId(), targetNode),
                    TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
                    new ActionListenerResponseHandler<>(listener, TrackShardResponse::new)
                );
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return retryableException(e);
            }
        };
        retryableAction.run();
    }

    private static boolean retryableException(Exception e) {
        if (e instanceof ConnectTransportException) {
            return true;
        } else if (e instanceof SendRequestTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof ConnectTransportException;
        } else if (e instanceof RemoteTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException
                || cause instanceof OpenSearchRejectedExecutionException
                || cause instanceof DelayRecoveryException;
        }
        return false;
    }

    private void setupReplicaShard(IndexShard indexShard) throws IndexShardRecoveryException {
        indexShard.prepareForIndexRecovery();
        final Store store = indexShard.store();
        store.incRef();
        try {
            store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion);
            final String translogUUID = Translog.createEmptyTranslog(
                indexShard.shardPath().resolveTranslog(),
                SequenceNumbers.NO_OPS_PERFORMED,
                indexShard.shardId(),
                indexShard.getPendingPrimaryTerm()
            );
            store.associateIndexWithNewTranslog(translogUUID);
            indexShard.persistRetentionLeases();
            indexShard.openEngineAndSkipTranslogRecovery();
        } catch (EngineException | IOException e) {
            throw new IndexShardRecoveryException(indexShard.shardId(), "failed to start replica shard", e);
        } finally {
            store.decRef();
        }
    }

    public void startReplication(
        final ReplicationCheckpoint checkpoint,
        final IndexShard indexShard,
        PrimaryShardReplicationSource source,
        final SegmentReplicationListener listener
    ) {
        indexShard.markAsReplicating();
        SegmentReplicationTarget target = new SegmentReplicationTarget(checkpoint, indexShard, source, listener);
        final long replicationId = onGoingReplications.startReplication(
            target,
            recoverySettings.activityTimeout()
        );
        logger.trace("Starting replication {}", replicationId);
        threadPool.generic().execute(new ReplicationRunner(replicationId));
    }

    private void doReplication(final long replicationId) {
        final Timer timer;
        try (ReplicationRef<SegmentReplicationTarget> replicationRef = onGoingReplications.getReplication(replicationId)) {
            if (replicationRef == null) {
                logger.trace("not running replication with id [{}] - can not find it (probably finished)", replicationId);
                return;
            }
            final SegmentReplicationTarget replicationTarget = replicationRef.get();
            timer = replicationTarget.state().getTimer();
            final IndexShard indexShard = replicationTarget.indexShard();

            try {
                logger.trace("{} preparing shard for replication", indexShard.shardId());
            } catch (final Exception e) {
                // this will be logged as warning later on...
                logger.error("unexpected error while preparing shard for peer replication, failing replication", e);
                onGoingReplications.failReplication(
                    replicationId,
                    new ReplicationFailedException(indexShard, "failed to prepare shard for replication", e),
                    true
                );
                return;
            }
            ReplicationResponseHandler listener = new ReplicationResponseHandler(replicationId, indexShard, timer);
            replicationTarget.startReplication(listener);
        }
    }

    /**
     * Start the recovery of a shard using Segment Replication.  This method will first setup the shard and then start segment copy.
     *
     * @param indexShard          {@link IndexShard} The target IndexShard.
     * @param targetNode          {@link DiscoveryNode} The IndexShard's DiscoveryNode
     * @param sourceNode          {@link DiscoveryNode} The source node.
     * @param replicationSource   {@link PrimaryShardReplicationSource} The source from where segments will be retrieved.
     * @param replicationListener {@link ReplicationListener} listener.
     */
    public void startRecovery(
        IndexShard indexShard,
        DiscoveryNode targetNode,
        DiscoveryNode sourceNode,
        PrimaryShardReplicationSource replicationSource,
        SegmentReplicationListener replicationListener
    ) {
        indexShard.markAsReplicating();
        StepListener<TrackShardResponse> trackShardListener = new StepListener<>();
        trackShardListener.whenComplete(
            r -> {
                startReplication(indexShard.getLatestReplicationCheckpoint(), indexShard, replicationSource, replicationListener);
            },
            e -> {
                replicationListener.onFailure(indexShard.getReplicationState(), new ReplicationFailedException(indexShard, e), true);
            }
        );
        prepareForReplication(indexShard, targetNode, sourceNode, trackShardListener);
    }

    class ReplicationRunner extends AbstractRunnable {

        final long replicationId;

        ReplicationRunner(long replicationId) {
            this.replicationId = replicationId;
        }

        @Override
        public void onFailure(Exception e) {
            try (ReplicationRef<SegmentReplicationTarget> replicationRef = onGoingReplications.getReplication(replicationId)) {
                if (replicationRef != null) {
                    logger.error(
                        () -> new ParameterizedMessage("unexpected error during replication [{}], failing shard", replicationId),
                        e
                    );
                    SegmentReplicationTarget replicationTarget = replicationRef.get();
                    onGoingReplications.failReplication(
                        replicationId,
                        new ReplicationFailedException(replicationTarget.indexShard(), "unexpected error", e),
                        true // be safe
                    );
                } else {
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "unexpected error during replication, but replication id [{}] is finished",
                            replicationId
                        ),
                        e
                    );
                }
            }
        }

        @Override
        public void doRun() {
            doReplication(replicationId);
        }
    }

    private class ReplicationResponseHandler implements ActionListener<ReplicationResponse> {

        private final long replicationId;
        private final IndexShard shard;
        private final Timer timer;

        private ReplicationResponseHandler(final long id, final IndexShard shard, final Timer timer) {
            this.replicationId = id;
            this.timer = timer;
            this.shard = shard;
        }

        @Override
        public void onResponse(ReplicationResponse replicationResponse) {
            // final TimeValue replicationTime = new TimeValue(timer.time());
            logger.trace("Replication complete {}", replicationId);
            if (shard.state() != IndexShardState.STARTED) {
                // The first time our shard is set up we need to mark its recovery complete.
                shard.recoveryState().getIndex().setFileDetailsComplete();
                shard.finalizeRecovery();
                shard.postRecovery("Shard setup complete.");
            }
            onGoingReplications.markReplicationAsDone(replicationId);
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Error", e);
            if (logger.isTraceEnabled()) {
                logger.trace(
                    () -> new ParameterizedMessage(
                        "[{}][{}] Got exception on replication",
                        shard.shardId().getIndex().getName(),
                        shard.shardId().id()
                    ),
                    e
                );
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof CancellableThreads.ExecutionCancelledException) {
                // this can also come from the source wrapped in a RemoteTransportException
                onGoingReplications.failReplication(
                    replicationId,
                    new ReplicationFailedException(shard, "source has canceled the replication", cause),
                    false
                );
                return;
            }
            onGoingReplications.failReplication(replicationId, new ReplicationFailedException(shard, e), true);
        }
    }

    public interface SegmentReplicationListener extends ReplicationListener {

        @Override
        default void onDone(ReplicationState state) {
            onReplicationDone((SegmentReplicationState) state);
        }

        @Override
        default void onFailure(ReplicationState state, OpenSearchException e, boolean sendShardFailure) {
            onReplicationFailure((SegmentReplicationState) state, (ReplicationFailedException) e, sendShardFailure);
        }

        void onReplicationDone(SegmentReplicationState state);

        void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure);
    }
}
