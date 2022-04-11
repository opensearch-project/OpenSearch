/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.RateLimiter;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.RetryableAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.breaker.CircuitBreakingException;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoveryFileChunkRequest;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.SegmentReplicationReplicaService;
import org.opensearch.indices.replication.checkpoint.TransportCheckpointInfoResponse;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.SendRequestTransportException;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.indices.replication.copy.SegmentReplicationPrimaryService.Actions.GET_CHECKPOINT_INFO;
import static org.opensearch.indices.replication.copy.SegmentReplicationPrimaryService.Actions.GET_FILES;

/**
 * The source for replication where the source is the primary shard of a cluster.
 */
public class PrimaryShardReplicationSource {

    private static final Logger logger = LogManager.getLogger(PrimaryShardReplicationSource.class);
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = ConcurrentCollections.newConcurrentMap();
    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;
    private final SegmentReplicationReplicaService segmentReplicationReplicaService;

    // TODO: Segrep - Cancellation doesn't make sense here as it should be per replication event.
    private volatile boolean isCancelled = false;

    public static class Actions {
        public static final String FILE_CHUNK = "internal:index/shard/segrep/file_chunk";
    }

    public PrimaryShardReplicationSource(
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        RecoverySettings recoverySettings,
        SegmentReplicationReplicaService segmentReplicationReplicaShardService
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        this.threadPool = transportService.getThreadPool();
        this.segmentReplicationReplicaService = segmentReplicationReplicaShardService;

        transportService.registerRequestHandler(
            Actions.FILE_CHUNK,
            ThreadPool.Names.GENERIC,
            RecoveryFileChunkRequest::new,
            new FileChunkTransportRequestHandler()
        );
    }

    public void getCheckpointInfo(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        StepListener<TransportCheckpointInfoResponse> listener
    ) {
        final ShardId shardId = checkpoint.getShardId();
        DiscoveryNode primaryNode = getPrimaryNode(shardId);
        final DiscoveryNodes nodes = clusterService.state().nodes();
        final Writeable.Reader<TransportCheckpointInfoResponse> reader = TransportCheckpointInfoResponse::new;
        final ActionListener<TransportCheckpointInfoResponse> responseListener = ActionListener.map(listener, r -> r);
        StartReplicationRequest request = new StartReplicationRequest(
            replicationId,
            getAllocationId(shardId),
            nodes.getLocalNode(),
            checkpoint
        );
        executeRetryableAction(primaryNode, GET_CHECKPOINT_INFO, request, responseListener, reader);
    }

    public void getFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        StepListener<GetFilesResponse> listener
    ) {
        final ShardId shardId = checkpoint.getShardId();
        DiscoveryNode primaryNode = getPrimaryNode(shardId);
        final DiscoveryNodes nodes = clusterService.state().nodes();
        final Writeable.Reader<GetFilesResponse> reader = GetFilesResponse::new;
        final ActionListener<GetFilesResponse> responseListener = ActionListener.map(listener, r -> r);

        GetFilesRequest request = new GetFilesRequest(
            replicationId,
            getAllocationId(shardId),
            nodes.getLocalNode(),
            filesToFetch,
            checkpoint
        );
        executeRetryableAction(primaryNode, GET_FILES, request, responseListener, reader);
    }

    private String getAllocationId(ShardId shardId) {
        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        final IndexShard shard = indexService.getShard(shardId.getId());
        return shard.routingEntry().allocationId().getId();
    }

    private DiscoveryNode getPrimaryNode(ShardId shardId) {
        ClusterState state = clusterService.state();
        ShardRouting primaryShard = state.routingTable().shardRoutingTable(shardId).primaryShard();
        return state.nodes().get(primaryShard.currentNodeId());
    }

    private <T extends TransportResponse> void executeRetryableAction(
        DiscoveryNode sourceNode,
        String action,
        TransportRequest request,
        ActionListener<T> actionListener,
        Writeable.Reader<T> reader
    ) {
        final Object key = new Object();
        final ActionListener<T> removeListener = ActionListener.runBefore(actionListener, () -> onGoingRetryableActions.remove(key));
        final TimeValue initialDelay = TimeValue.timeValueMillis(200);
        final TimeValue timeout = recoverySettings.internalActionRetryTimeout();
        final TransportRequestOptions options = TransportRequestOptions.builder()
            .withTimeout(recoverySettings.internalActionTimeout())
            .build();
        final RetryableAction<T> retryableAction = new RetryableAction<T>(logger, threadPool, initialDelay, timeout, removeListener) {

            @Override
            public void tryAction(ActionListener<T> listener) {
                transportService.sendRequest(
                    sourceNode,
                    action,
                    request,
                    options,
                    new ActionListenerResponseHandler<>(listener, reader, ThreadPool.Names.GENERIC)
                );
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return retryableException(e);
            }
        };
        onGoingRetryableActions.put(key, retryableAction);
        retryableAction.run();
        if (isCancelled) {
            retryableAction.cancel(new CancellableThreads.ExecutionCancelledException("recovery was cancelled"));
        }
    }

    private static boolean retryableException(Exception e) {
        if (e instanceof ConnectTransportException) {
            return true;
        } else if (e instanceof SendRequestTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof ConnectTransportException;
        } else if (e instanceof RemoteTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException || cause instanceof OpenSearchRejectedExecutionException;
        }
        return false;
    }

    public void cancel() {
        isCancelled = true;
        if (onGoingRetryableActions.isEmpty()) {
            return;
        }
        final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("recovery was cancelled");
        // Dispatch to generic as cancellation calls can come on the cluster state applier thread
        threadPool.generic().execute(() -> {
            for (RetryableAction<?> action : onGoingRetryableActions.values()) {
                action.cancel(exception);
            }
            onGoingRetryableActions.clear();
        });
    }

    class FileChunkTransportRequestHandler implements TransportRequestHandler<RecoveryFileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final RecoveryFileChunkRequest request, TransportChannel channel, Task task) throws Exception {
            try (
                ReplicationCollection.ReplicationRef replicationRef = segmentReplicationReplicaService.getOnGoingReplications()
                    .getReplicationSafe(request.replicationId(), request.shardId())
            ) {
                final SegmentReplicationTarget replicationTarget = replicationRef.get();
                final ActionListener<Void> listener = createOrFinishListener(replicationRef, channel, Actions.FILE_CHUNK, request);
                if (listener == null) {
                    return;
                }

                // final ReplicationState.Index indexState = replicationTarget.state().getIndex();
                // if (request.sourceThrottleTimeInNanos() != ReplicationState.Index.UNKNOWN) {
                // indexState.addSourceThrottling(request.sourceThrottleTimeInNanos());
                // }

                RateLimiter rateLimiter = recoverySettings.rateLimiter();
                if (rateLimiter != null) {
                    long bytes = bytesSinceLastPause.addAndGet(request.content().length());
                    if (bytes > rateLimiter.getMinPauseCheckBytes()) {
                        // Time to pause
                        bytesSinceLastPause.addAndGet(-bytes);
                        // long throttleTimeInNanos = rateLimiter.pause(bytes);
                        // indexState.addTargetThrottling(throttleTimeInNanos);
                        // replicationTarget.getIndexShard().replicationStats().addThrottleTime(throttleTimeInNanos);
                    }
                }
                replicationTarget.writeFileChunk(request.metadata(), request.position(), request.content(), request.lastChunk(), listener);
            }
        }
    }

    private ActionListener<Void> createOrFinishListener(
        final ReplicationCollection.ReplicationRef replicationRef,
        final TransportChannel channel,
        final String action,
        final RecoveryFileChunkRequest request
    ) {
        return createOrFinishListener(replicationRef, channel, action, request, nullVal -> TransportResponse.Empty.INSTANCE);
    }

    private ActionListener<Void> createOrFinishListener(
        final ReplicationCollection.ReplicationRef replicationRef,
        final TransportChannel channel,
        final String action,
        final RecoveryFileChunkRequest request,
        final CheckedFunction<Void, TransportResponse, Exception> responseFn
    ) {
        final SegmentReplicationTarget replicationTarget = replicationRef.get();
        final ActionListener<TransportResponse> channelListener = new ChannelActionListener<>(channel, action, request);
        final ActionListener<Void> voidListener = ActionListener.map(channelListener, responseFn);

        final long requestSeqNo = request.requestSeqNo();
        final ActionListener<Void> listener;
        if (requestSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            listener = replicationTarget.markRequestReceivedAndCreateListener(requestSeqNo, voidListener);
        } else {
            listener = voidListener;
        }

        return listener;
    }
}
