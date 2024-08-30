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
import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.recovery.ForceSyncRequest;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RetryableTransportClient;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationCollection.ReplicationRef;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.UPDATE_VISIBLE_CHECKPOINT;

/**
 * Service class that orchestrates replication events on replicas.
 *
 * @opensearch.internal
 */
public class SegmentReplicationTargetService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationTargetService.class);

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;

    private final ReplicationCollection<SegmentReplicationTarget> onGoingReplications;

    private final Map<ShardId, SegmentReplicationState> completedReplications = ConcurrentCollections.newConcurrentMap();

    private final SegmentReplicationSourceFactory sourceFactory;

    protected final Map<ShardId, ReplicationCheckpoint> latestReceivedCheckpoint = ConcurrentCollections.newConcurrentMap();

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;

    /**
     * The internal actions
     *
     * @opensearch.internal
     */
    public static class Actions {
        public static final String FILE_CHUNK = "internal:index/shard/replication/file_chunk";
        public static final String FORCE_SYNC = "internal:index/shard/replication/segments_sync";
    }

    public SegmentReplicationTargetService(
        final ThreadPool threadPool,
        final RecoverySettings recoverySettings,
        final TransportService transportService,
        final SegmentReplicationSourceFactory sourceFactory,
        final IndicesService indicesService,
        final ClusterService clusterService
    ) {
        this(
            threadPool,
            recoverySettings,
            transportService,
            sourceFactory,
            indicesService,
            clusterService,
            new ReplicationCollection<>(logger, threadPool)
        );
    }

    public SegmentReplicationTargetService(
        final ThreadPool threadPool,
        final RecoverySettings recoverySettings,
        final TransportService transportService,
        final SegmentReplicationSourceFactory sourceFactory,
        final IndicesService indicesService,
        final ClusterService clusterService,
        final ReplicationCollection<SegmentReplicationTarget> ongoingSegmentReplications
    ) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.onGoingReplications = ongoingSegmentReplications;
        this.sourceFactory = sourceFactory;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;

        transportService.registerRequestHandler(
            Actions.FILE_CHUNK,
            ThreadPool.Names.GENERIC,
            FileChunkRequest::new,
            new FileChunkTransportRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.FORCE_SYNC,
            ThreadPool.Names.GENERIC,
            ForceSyncRequest::new,
            new ForceSyncTransportRequestHandler()
        );
    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            assert onGoingReplications.size() == 0 : "Replication collection should be empty on shutdown";
            clusterService.removeListener(this);
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.routingTableChanged()) {
            for (IndexService indexService : indicesService) {
                if (indexService.getIndexSettings().isSegRepEnabledOrRemoteNode()
                    && event.indexRoutingTableChanged(indexService.index().getName())) {
                    for (IndexShard shard : indexService) {
                        if (shard.routingEntry().primary() == false && shard.routingEntry().isSearchOnly() == false) {
                            // for this shard look up its primary routing, if it has completed a relocation trigger replication
                            final String previousNode = event.previousState()
                                .routingTable()
                                .shardRoutingTable(shard.shardId())
                                .primaryShard()
                                .currentNodeId();
                            final String currentNode = event.state()
                                .routingTable()
                                .shardRoutingTable(shard.shardId())
                                .primaryShard()
                                .currentNodeId();
                            if (previousNode.equals(currentNode) == false) {
                                processLatestReceivedCheckpoint(shard, Thread.currentThread());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Cancel any replications on this node for a replica that is about to be closed.
     */
    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null && indexShard.indexSettings().isSegRepEnabledOrRemoteNode()) {
            onGoingReplications.cancelForShard(indexShard.shardId(), "Shard closing");
            latestReceivedCheckpoint.remove(shardId);
        }
    }

    /**
     * Replay any received checkpoint while replica was recovering.  This does not need to happen
     * for primary relocations because they recover from translog.
     */
    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        if (indexShard.indexSettings().isSegRepEnabledOrRemoteNode() && indexShard.routingEntry().primary() == false) {
            processLatestReceivedCheckpoint(indexShard, Thread.currentThread());
        }
    }

    /**
     * Cancel any replications on this node for a replica that has just been promoted as the new primary.
     */
    @Override
    public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {
        if (oldRouting != null
            && indexShard.indexSettings().isSegRepEnabledOrRemoteNode()
            && oldRouting.primary() == false
            && newRouting.primary()) {
            onGoingReplications.cancelForShard(indexShard.shardId(), "Shard has been promoted to primary");
            latestReceivedCheckpoint.remove(indexShard.shardId());
        }
    }

    /**
     * returns SegmentReplicationState of on-going segment replication events.
     */
    @Nullable
    public SegmentReplicationState getOngoingEventSegmentReplicationState(ShardId shardId) {
        return Optional.ofNullable(onGoingReplications.getOngoingReplicationTarget(shardId))
            .map(SegmentReplicationTarget::state)
            .orElse(null);
    }

    /**
     * returns SegmentReplicationState of latest completed segment replication events.
     */
    @Nullable
    public SegmentReplicationState getlatestCompletedEventSegmentReplicationState(ShardId shardId) {
        return completedReplications.get(shardId);
    }

    /**
     * returns SegmentReplicationState of on-going if present or completed segment replication events.
     */
    @Nullable
    public SegmentReplicationState getSegmentReplicationState(ShardId shardId) {
        return Optional.ofNullable(getOngoingEventSegmentReplicationState(shardId))
            .orElseGet(() -> getlatestCompletedEventSegmentReplicationState(shardId));
    }

    public ReplicationRef<SegmentReplicationTarget> get(long replicationId) {
        return onGoingReplications.get(replicationId);
    }

    public SegmentReplicationTarget get(ShardId shardId) {
        return onGoingReplications.getOngoingReplicationTarget(shardId);
    }

    /**
     * Invoked when a new checkpoint is received from a primary shard.
     * It checks if a new checkpoint should be processed or not and starts replication if needed.
     *
     * @param receivedCheckpoint received checkpoint that is checked for processing
     * @param replicaShard       replica shard on which checkpoint is received
     */
    public synchronized void onNewCheckpoint(final ReplicationCheckpoint receivedCheckpoint, final IndexShard replicaShard) {
        logger.debug(() -> new ParameterizedMessage("Replica received new replication checkpoint from primary [{}]", receivedCheckpoint));
        // if the shard is in any state
        if (replicaShard.state().equals(IndexShardState.CLOSED)) {
            // ignore if shard is closed
            logger.trace(() -> "Ignoring checkpoint, Shard is closed");
            return;
        }
        updateLatestReceivedCheckpoint(receivedCheckpoint, replicaShard);
        // Checks if replica shard is in the correct STARTED state to process checkpoints (avoids parallel replication events taking place)
        // This check ensures we do not try to process a received checkpoint while the shard is still recovering, yet we stored the latest
        // checkpoint to be replayed once the shard is Active.
        if (replicaShard.state().equals(IndexShardState.STARTED) == true) {
            // Checks if received checkpoint is already present and ahead then it replaces old received checkpoint
            SegmentReplicationTarget ongoingReplicationTarget = onGoingReplications.getOngoingReplicationTarget(replicaShard.shardId());
            if (ongoingReplicationTarget != null) {
                if (ongoingReplicationTarget.getCheckpoint().getPrimaryTerm() < receivedCheckpoint.getPrimaryTerm()) {
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "Cancelling ongoing replication {} from old primary with primary term {}",
                            ongoingReplicationTarget.description(),
                            ongoingReplicationTarget.getCheckpoint().getPrimaryTerm()
                        )
                    );
                    ongoingReplicationTarget.cancel("Cancelling stuck target after new primary");
                } else {
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "Ignoring new replication checkpoint - shard is currently replicating to checkpoint {}",
                            ongoingReplicationTarget.getCheckpoint()
                        )
                    );
                    return;
                }
            }
            final Thread thread = Thread.currentThread();
            if (replicaShard.shouldProcessCheckpoint(receivedCheckpoint)) {
                startReplication(replicaShard, receivedCheckpoint, new SegmentReplicationListener() {
                    @Override
                    public void onReplicationDone(SegmentReplicationState state) {
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "[shardId {}] [replication id {}] Replication complete to {}, timing data: {}",
                                replicaShard.shardId().getId(),
                                state.getReplicationId(),
                                replicaShard.getLatestReplicationCheckpoint(),
                                state.getTimingData()
                            )
                        );

                        // update visible checkpoint to primary
                        updateVisibleCheckpoint(state.getReplicationId(), replicaShard);

                        // if we received a checkpoint during the copy event that is ahead of this
                        // try and process it.
                        processLatestReceivedCheckpoint(replicaShard, thread);
                    }

                    @Override
                    public void onReplicationFailure(
                        SegmentReplicationState state,
                        ReplicationFailedException e,
                        boolean sendShardFailure
                    ) {
                        logReplicationFailure(state, e, replicaShard);
                        if (sendShardFailure == true) {
                            failShard(e, replicaShard);
                        } else {
                            processLatestReceivedCheckpoint(replicaShard, thread);
                        }
                    }
                });
            } else if (replicaShard.isSegmentReplicationAllowed()) {
                // if we didn't process the checkpoint because we are up to date,
                // send our latest checkpoint to the primary to update tracking.
                // replicationId is not used by the primary set to a default value.
                final long replicationId = NO_OPS_PERFORMED;
                updateVisibleCheckpoint(replicationId, replicaShard);
            }
        } else {
            logger.trace(
                () -> new ParameterizedMessage("Ignoring checkpoint, shard not started {} {}", receivedCheckpoint, replicaShard.state())
            );
        }
    }

    private void logReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, IndexShard replicaShard) {
        // only log as error if error is not a cancellation.
        if (ExceptionsHelper.unwrap(e, CancellableThreads.ExecutionCancelledException.class) == null) {
            logger.error(
                () -> new ParameterizedMessage(
                    "[shardId {}] [replication id {}] Replication failed, timing data: {}",
                    replicaShard.shardId(),
                    state.getReplicationId(),
                    state.getTimingData()
                ),
                e
            );
        } else {
            logger.debug(
                () -> new ParameterizedMessage(
                    "[shardId {}] [replication id {}] Replication cancelled",
                    replicaShard.shardId(),
                    state.getReplicationId()
                ),
                e
            );
        }
    }

    protected void updateVisibleCheckpoint(long replicationId, IndexShard replicaShard) {
        // Update replication checkpoint on source via transport call only supported for remote store integration. For node-
        // node communication, checkpoint update is piggy-backed to GET_SEGMENT_FILES transport call
        if (replicaShard.indexSettings().isAssignedOnRemoteNode() == false) {
            return;
        }
        ShardRouting primaryShard = clusterService.state().routingTable().shardRoutingTable(replicaShard.shardId()).primaryShard();

        final UpdateVisibleCheckpointRequest request = new UpdateVisibleCheckpointRequest(
            replicationId,
            replicaShard.routingEntry().allocationId().getId(),
            primaryShard.shardId(),
            getPrimaryNode(primaryShard),
            replicaShard.getLatestReplicationCheckpoint()
        );

        final TransportRequestOptions options = TransportRequestOptions.builder()
            .withTimeout(recoverySettings.internalActionTimeout())
            .build();
        logger.trace(
            () -> new ParameterizedMessage(
                "Updating Primary shard that replica {}-{} is synced to checkpoint {}",
                replicaShard.shardId(),
                replicaShard.routingEntry().allocationId(),
                request.getCheckpoint()
            )
        );
        RetryableTransportClient transportClient = new RetryableTransportClient(
            transportService,
            getPrimaryNode(primaryShard),
            recoverySettings.internalActionRetryTimeout(),
            logger
        );
        final ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.trace(
                    () -> new ParameterizedMessage(
                        "Successfully updated replication checkpoint {} for replica {}",
                        replicaShard.shardId(),
                        request.getCheckpoint()
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "Failed to update visible checkpoint for replica {}, {}:",
                        replicaShard.shardId(),
                        request.getCheckpoint()
                    ),
                    e
                );
            }
        };

        transportClient.executeRetryableAction(
            UPDATE_VISIBLE_CHECKPOINT,
            request,
            options,
            ActionListener.map(listener, r -> null),
            in -> TransportResponse.Empty.INSTANCE
        );
    }

    private DiscoveryNode getPrimaryNode(ShardRouting primaryShard) {
        return clusterService.state().nodes().get(primaryShard.currentNodeId());
    }

    // visible to tests
    protected boolean processLatestReceivedCheckpoint(IndexShard replicaShard, Thread thread) {
        final ReplicationCheckpoint latestPublishedCheckpoint = latestReceivedCheckpoint.get(replicaShard.shardId());
        if (latestPublishedCheckpoint != null) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "Processing latest received checkpoint for shard {} {}",
                    replicaShard.shardId(),
                    latestPublishedCheckpoint
                )
            );
            Runnable runnable = () -> {
                // if we retry ensure the shard is not in the process of being closed.
                // it will be removed from indexService's collection before the shard is actually marked as closed.
                if (indicesService.getShardOrNull(replicaShard.shardId()) != null) {
                    onNewCheckpoint(latestReceivedCheckpoint.get(replicaShard.shardId()), replicaShard);
                }
            };
            // Checks if we are using same thread and forks if necessary.
            if (thread == Thread.currentThread()) {
                threadPool.generic().execute(runnable);
            } else {
                runnable.run();
            }
            return true;
        }
        return false;
    }

    // visible to tests
    protected void updateLatestReceivedCheckpoint(ReplicationCheckpoint receivedCheckpoint, IndexShard replicaShard) {
        if (latestReceivedCheckpoint.get(replicaShard.shardId()) != null) {
            if (receivedCheckpoint.isAheadOf(latestReceivedCheckpoint.get(replicaShard.shardId()))) {
                latestReceivedCheckpoint.replace(replicaShard.shardId(), receivedCheckpoint);
            }
        } else {
            latestReceivedCheckpoint.put(replicaShard.shardId(), receivedCheckpoint);
        }
    }

    /**
     * Start a round of replication and sync to at least the given checkpoint.
     * @param indexShard - {@link IndexShard} replica shard
     * @param checkpoint - {@link ReplicationCheckpoint} checkpoint to sync to
     * @param listener - {@link ReplicationListener}
     * @return {@link SegmentReplicationTarget} target event orchestrating the event.
     */
    public SegmentReplicationTarget startReplication(
        final IndexShard indexShard,
        final ReplicationCheckpoint checkpoint,
        final SegmentReplicationListener listener
    ) {
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            indexShard,
            checkpoint,
            sourceFactory.get(indexShard),
            listener
        );
        startReplication(target);
        return target;
    }

    // pkg-private for integration tests
    void startReplication(final SegmentReplicationTarget target) {
        final long replicationId;
        try {
            replicationId = onGoingReplications.startSafe(target, recoverySettings.activityTimeout());
        } catch (ReplicationFailedException e) {
            // replication already running for shard.
            target.fail(e, false);
            return;
        }
        logger.trace(() -> new ParameterizedMessage("Added new replication to collection {}", target.description()));
        threadPool.generic().execute(new ReplicationRunner(replicationId));
    }

    /**
     * Listener that runs on changes in Replication state
     *
     * @opensearch.internal
     */
    public interface SegmentReplicationListener extends ReplicationListener {

        @Override
        default void onDone(ReplicationState state) {
            onReplicationDone((SegmentReplicationState) state);
        }

        @Override
        default void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
            onReplicationFailure((SegmentReplicationState) state, e, sendShardFailure);
        }

        void onReplicationDone(SegmentReplicationState state);

        void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure);
    }

    /**
     * Runnable implementation to trigger a replication event.
     */
    private class ReplicationRunner extends AbstractRunnable {

        final long replicationId;

        public ReplicationRunner(long replicationId) {
            this.replicationId = replicationId;
        }

        @Override
        public void onFailure(Exception e) {
            onGoingReplications.fail(replicationId, new ReplicationFailedException("Unexpected Error during replication", e), false);
        }

        @Override
        public void doRun() {
            start(replicationId);
        }
    }

    private void start(final long replicationId) {
        final SegmentReplicationTarget target;
        try (ReplicationRef<SegmentReplicationTarget> replicationRef = onGoingReplications.get(replicationId)) {
            // This check is for handling edge cases where the reference is removed before the ReplicationRunner is started by the
            // threadpool.
            if (replicationRef == null) {
                return;
            }
            target = replicationRef.get();
        }
        target.startReplication(new ActionListener<>() {
            @Override
            public void onResponse(Void o) {
                logger.debug(() -> new ParameterizedMessage("Finished replicating {} marking as done.", target.description()));
                onGoingReplications.markAsDone(replicationId);
                if (target.state().getIndex().recoveredFileCount() != 0 && target.state().getIndex().recoveredBytes() != 0) {
                    completedReplications.put(target.shardId(), target.state());
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Replication failed {}", target.description());
                if (isStoreCorrupt(target) || e instanceof CorruptIndexException || e instanceof OpenSearchCorruptionException) {
                    onGoingReplications.fail(replicationId, new ReplicationFailedException("Store corruption during replication", e), true);
                    return;
                }
                onGoingReplications.fail(replicationId, new ReplicationFailedException("Segment Replication failed", e), false);
            }
        });
    }

    private boolean isStoreCorrupt(SegmentReplicationTarget target) {
        // ensure target is not already closed. In that case
        // we can assume the store is not corrupt and that the replication
        // event completed successfully.
        if (target.refCount() > 0) {
            final Store store = target.store();
            if (store.tryIncRef()) {
                try {
                    return store.isMarkedCorrupted();
                } catch (IOException ex) {
                    logger.warn("Unable to determine if store is corrupt", ex);
                    return false;
                } finally {
                    store.decRef();
                }
            }
        }
        // store already closed.
        return false;
    }

    private class FileChunkTransportRequestHandler implements TransportRequestHandler<FileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final FileChunkRequest request, TransportChannel channel, Task task) throws Exception {
            try (ReplicationRef<SegmentReplicationTarget> ref = onGoingReplications.getSafe(request.recoveryId(), request.shardId())) {
                final SegmentReplicationTarget target = ref.get();
                final ActionListener<Void> listener = target.createOrFinishListener(channel, Actions.FILE_CHUNK, request);
                target.handleFileChunk(request, target, bytesSinceLastPause, recoverySettings.replicationRateLimiter(), listener);
            }
        }
    }

    /**
     * Force sync transport handler forces round of segment replication. Caller should verify necessary checks before
     * calling this handler.
     */
    private class ForceSyncTransportRequestHandler implements TransportRequestHandler<ForceSyncRequest> {
        @Override
        public void messageReceived(final ForceSyncRequest request, TransportChannel channel, Task task) throws Exception {
            forceReplication(request, new ChannelActionListener<>(channel, Actions.FORCE_SYNC, request));
        }
    }

    private void forceReplication(ForceSyncRequest request, ActionListener<TransportResponse> listener) {
        final ShardId shardId = request.getShardId();
        assert indicesService != null;
        final IndexShard indexShard = indicesService.getShardOrNull(shardId);
        // Proceed with round of segment replication only when it is allowed
        if (indexShard == null || indexShard.getReplicationEngine().isEmpty()) {
            listener.onResponse(TransportResponse.Empty.INSTANCE);
        } else {
            // We are skipping any validation for an incoming checkpoint, use the shard's latest checkpoint in the target.
            startReplication(
                indexShard,
                indexShard.getLatestReplicationCheckpoint(),
                new SegmentReplicationTargetService.SegmentReplicationListener() {
                    @Override
                    public void onReplicationDone(SegmentReplicationState state) {
                        try {
                            logger.trace(
                                () -> new ParameterizedMessage(
                                    "[shardId {}] [replication id {}] Force replication Sync complete to {}, timing data: {}",
                                    shardId,
                                    state.getReplicationId(),
                                    indexShard.getLatestReplicationCheckpoint(),
                                    state.getTimingData()
                                )
                            );
                            // Promote engine type for primary target
                            if (indexShard.recoveryState().getPrimary() == true) {
                                indexShard.resetToWriteableEngine();
                            } else {
                                // Update the replica's checkpoint on primary's replication tracker.
                                updateVisibleCheckpoint(state.getReplicationId(), indexShard);
                            }
                            listener.onResponse(TransportResponse.Empty.INSTANCE);
                        } catch (Exception e) {
                            logger.error("Error while marking replication completed", e);
                            listener.onFailure(e);
                        }
                    }

                    @Override
                    public void onReplicationFailure(
                        SegmentReplicationState state,
                        ReplicationFailedException e,
                        boolean sendShardFailure
                    ) {
                        logReplicationFailure(state, e, indexShard);
                        if (sendShardFailure) {
                            failShard(e, indexShard);
                        }
                        listener.onFailure(e);
                    }
                }
            );
        }
    }

    private void failShard(ReplicationFailedException e, IndexShard indexShard) {
        try {
            indexShard.failShard("unrecoverable replication failure", e);
        } catch (Exception inner) {
            logger.error("Error attempting to fail shard", inner);
            e.addSuppressed(inner);
        }
    }

}
