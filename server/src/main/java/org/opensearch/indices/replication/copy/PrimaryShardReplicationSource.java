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
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.RetryableAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.breaker.CircuitBreakingException;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.TransportCheckpointInfoResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.SendRequestTransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.Map;

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

    // TODO: Segrep - Cancellation doesn't make sense here as it should be per replication event.
    private volatile boolean isCancelled = false;

    public PrimaryShardReplicationSource(
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        RecoverySettings recoverySettings
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        this.threadPool = transportService.getThreadPool();
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
}
