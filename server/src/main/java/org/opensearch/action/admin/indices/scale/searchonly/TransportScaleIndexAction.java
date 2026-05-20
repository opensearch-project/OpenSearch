/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_SEARCH_ONLY_BLOCK_ID;

/**
 * Transport action implementation for search-only scale operations.
 * <p>
 * This class coordinates the entire process of scaling indices up or down between normal
 * and search-only modes. It manages the multistep process including:
 * <ul>
 *   <li>Validating prerequisites for scale operations</li>
 *   <li>Adding temporary write blocks during scale-down preparation</li>
 *   <li>Coordinating shard synchronization across nodes</li>
 *   <li>Modifying cluster state to transition indices between modes</li>
 *   <li>Handling synchronization requests from other nodes</li>
 * </ul>
 * <p>
 * The scale operation is implemented as a series of cluster state update tasks to ensure
 * atomicity and consistency throughout the transition.
 */
public class TransportScaleIndexAction extends TransportClusterManagerNodeAction<ScaleIndexRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportScaleIndexAction.class);
    /**
     * Transport action name for shard sync requests
     */
    public static final String NAME = ScaleIndexAction.NAME + "[s]";

    public static final String SHARD_SYNC_EXECUTOR = ThreadPool.Names.MANAGEMENT;

    private final AllocationService allocationService;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;

    private final ScaleIndexOperationValidator validator;
    private final ScaleIndexClusterStateBuilder scaleIndexClusterStateBuilder;
    private final ScaleIndexShardSyncManager scaleIndexShardSyncManager;

    /**
     * Constructs a new TransportSearchOnlyAction.
     *
     * @param transportService            the transport service for network communication
     * @param clusterService              the cluster service for accessing cluster state
     * @param threadPool                  the thread pool for executing operations
     * @param actionFilters               filters for action requests
     * @param indexNameExpressionResolver resolver for index names and expressions
     * @param allocationService           service for shard allocation decisions
     * @param indicesService              service for accessing index shards
     */
    @Inject
    public TransportScaleIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        super(
            ScaleIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ScaleIndexRequest::new,
            indexNameExpressionResolver
        );
        this.allocationService = allocationService;
        this.indicesService = indicesService;
        this.threadPool = threadPool;
        this.validator = new ScaleIndexOperationValidator();
        this.scaleIndexClusterStateBuilder = new ScaleIndexClusterStateBuilder();
        this.scaleIndexShardSyncManager = new ScaleIndexShardSyncManager(clusterService, transportService, NAME);

        transportService.registerRequestHandler(
            NAME,
            ThreadPool.Names.SAME,
            ScaleIndexNodeRequest::new,
            (request, channel, task) -> handleShardSyncRequest(request, channel)
        );
    }

    /**
     * Returns the executor name for this transport action.
     *
     * @return the executor name
     */
    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    /**
     * Deserializes the response from stream.
     *
     * @param in the stream input
     * @return the deserialized response
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    /**
     * Handles the search-only request on the cluster manager node.
     * <p>
     * This method determines whether to execute a scale-up or scale-down operation
     * based on the request parameters, and submits the appropriate cluster state update task.
     *
     * @param request  the search-only scale request
     * @param state    the current cluster state
     * @param listener the listener to notify with the operation result
     */
    @Override
    protected void clusterManagerOperation(ScaleIndexRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        try {
            String index = request.getIndex();
            if (request.isScaleDown()) {
                submitScaleDownTask(index, listener);
            } else {
                submitScaleUpTask(index, state, listener);
            }
        } catch (Exception e) {
            logger.error("Failed to execute cluster manager operation", e);
            listener.onFailure(e);
        }
    }

    /**
     * Submits the scale-down update task: it first adds a temporary block to the indices and then initiates shard synchronization.
     */
    private void submitScaleDownTask(final String index, final ActionListener<AcknowledgedResponse> listener) {
        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

        clusterService.submitStateUpdateTask(
            "add-block-index-to-scale " + index,
            new AddBlockClusterStateUpdateTask(index, blockedIndices, listener)
        );
    }

    /**
     * Sends shard sync requests to each node that holds a primary shard.
     */
    private void proceedWithScaleDown(
        String index,
        Map<ShardId, String> primaryShardsNodes,
        ActionListener<AcknowledgedResponse> listener
    ) {
        scaleIndexShardSyncManager.sendShardSyncRequests(
            index,
            primaryShardsNodes,
            ActionListener.wrap(responses -> handleShardSyncResponses(responses, index, listener), listener::onFailure)
        );
    }

    private void handleShardSyncResponses(
        Collection<ScaleIndexNodeResponse> responses,
        String index,
        ActionListener<AcknowledgedResponse> listener
    ) {
        scaleIndexShardSyncManager.validateNodeResponses(
            responses,
            ActionListener.wrap(searchOnlyResponse -> finalizeScaleDown(index, listener), listener::onFailure)
        );
    }

    /**
     * Finalizes scale-down by updating the metadata and routing table:
     * removes the temporary block and adds a permanent search-only block.
     */
    private void finalizeScaleDown(String index, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("finalize-scale-down", new FinalizeScaleDownTask(index, listener));
    }

    /**
     * Handles an incoming shard sync request from another node.
     */
    void handleShardSyncRequest(ScaleIndexNodeRequest request, TransportChannel channel) {
        ClusterState state = clusterService.state();

        IndexMetadata indexMetadata = state.metadata().index(request.getIndex());
        if (indexMetadata == null) {
            throw new IllegalStateException("Index " + request.getIndex() + " not found");
        }

        IndexService indexService = getIndexService(indexMetadata);
        ChannelActionListener<ScaleIndexNodeResponse, ScaleIndexNodeRequest> listener = new ChannelActionListener<>(
            channel,
            "sync_shard",
            request
        );

        syncShards(indexService, request.getShardIds(), listener);
    }

    private IndexService getIndexService(IndexMetadata indexMetadata) {
        IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
        if (indexService == null) {
            throw new IllegalStateException("IndexService not found for index " + indexMetadata.getIndex().getName());
        }
        return indexService;
    }

    private void syncShards(IndexService indexService, List<ShardId> shardIds, ActionListener<ScaleIndexNodeResponse> listener) {

        GroupedActionListener<ScaleIndexShardResponse> groupedActionListener = new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Collection<ScaleIndexShardResponse> shardResponses) {
                listener.onResponse(new ScaleIndexNodeResponse(clusterService.localNode(), shardResponses.stream().toList()));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }, shardIds.size());

        for (ShardId shardId : shardIds) {
            IndexShard shard = indexService.getShardOrNull(shardId.id());
            if (shard == null || shard.routingEntry().primary() == false) {
                groupedActionListener.onFailure(new IllegalStateException("Attempting to scale down a replica shard"));
                break;
            }
            threadPool.executor(SHARD_SYNC_EXECUTOR).execute(() -> { syncSingleShard(shard, groupedActionListener); });
        }
    }

    void syncSingleShard(IndexShard shard, ActionListener<ScaleIndexShardResponse> listener) {
        shard.acquireAllPrimaryOperationsPermits(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                logger.info("Performing final sync and flush for shard {}", shard.shardId());
                try {
                    shard.sync();
                    shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
                    shard.waitForRemoteStoreSync();
                    listener.onResponse(
                        new ScaleIndexShardResponse(shard.shardId(), shard.isSyncNeeded(), shard.translogStats().getUncommittedOperations())
                    );
                } catch (IOException e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }, TimeValue.timeValueSeconds(30));
    }

    /**
     * Submits the scale-up update task that rebuilds the routing table and updates index metadata.
     */
    private void submitScaleUpTask(
        final String index,
        final ClusterState currentState,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        IndexMetadata indexMetadata = currentState.metadata().index(index);
        if (!validator.validateScalePrerequisites(indexMetadata, index, listener, false)) {
            return;
        }

        clusterService.submitStateUpdateTask("scale-up-index", new ScaleUpClusterStateUpdateTask(index, listener));
    }

    @Override
    protected ClusterBlockException checkBlock(ScaleIndexRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(state, request.indicesOptions(), request.getIndex())
            );
    }

    /**
     * Cluster state update task for adding a temporary block during the initial phase of scaling down.
     * <p>
     * This task:
     * <ul>
     *   <li>Validates that the index meets prerequisites for scaling down</li>
     *   <li>Adds a temporary write block to prevent new operations during scaling</li>
     *   <li>Initiates shard synchronization after the block is applied</li>
     * </ul>
     */
    class AddBlockClusterStateUpdateTask extends ClusterStateUpdateTask {
        private final String index;
        private final Map<Index, ClusterBlock> blockedIndices;
        private final ActionListener<AcknowledgedResponse> listener;

        AddBlockClusterStateUpdateTask(
            String index,
            Map<Index, ClusterBlock> blockedIndices,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(Priority.URGENT);
            this.index = index;
            this.blockedIndices = blockedIndices;
            this.listener = listener;
        }

        @Override
        public ClusterState execute(final ClusterState currentState) {
            IndexMetadata indexMetadata = currentState.metadata().index(index);
            try {
                validator.validateScalePrerequisites(indexMetadata, index, listener, true);
                return scaleIndexClusterStateBuilder.buildScaleDownState(currentState, index, blockedIndices);
            } catch (Exception e) {
                return currentState;
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            if (oldState == newState) {
                listener.onResponse(new AcknowledgedResponse(true));
                return;
            }

            IndexMetadata indexMetadata = newState.metadata().index(index);
            if (indexMetadata != null) {
                Map<ShardId, String> primaryShardsNodes = scaleIndexShardSyncManager.getPrimaryShardAssignments(indexMetadata, newState);
                proceedWithScaleDown(index, primaryShardsNodes, listener);
            }
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.error("Failed to process cluster state update for scale down", e);
            listener.onFailure(e);
        }
    }

    /**
     * Cluster state update task for finalizing a scale-down operation.
     * <p>
     * This task:
     * <ul>
     *   <li>Removes the temporary scale-down preparation block</li>
     *   <li>Updates index metadata to mark it as search-only</li>
     *   <li>Applies a permanent search-only block</li>
     *   <li>Updates the routing table to remove non-search-only shards</li>
     * </ul>
     */
    class FinalizeScaleDownTask extends ClusterStateUpdateTask {
        private final String index;
        private final ActionListener<AcknowledgedResponse> listener;

        FinalizeScaleDownTask(String index, ActionListener<AcknowledgedResponse> listener) {
            super(Priority.URGENT);
            this.index = index;
            this.listener = listener;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            return scaleIndexClusterStateBuilder.buildFinalScaleDownState(currentState, index);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            listener.onResponse(new AcknowledgedResponse(true));
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.error("Failed to finalize scale-down operation", e);
            listener.onFailure(e);
        }
    }

    /**
     * Cluster state update task for scaling up an index from search-only mode to normal operation.
     * <p>
     * This task:
     * <ul>
     *   <li>Rebuilds the routing table to add primary and replica shards</li>
     *   <li>Removes the search-only block</li>
     *   <li>Updates index settings to disable search-only mode</li>
     *   <li>Triggers routing table updates to allocate the new shards</li>
     * </ul>
     */
    private class ScaleUpClusterStateUpdateTask extends ClusterStateUpdateTask {
        private final String index;
        private final ActionListener<AcknowledgedResponse> listener;

        ScaleUpClusterStateUpdateTask(String index, ActionListener<AcknowledgedResponse> listener) {
            this.index = index;
            this.listener = listener;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            RoutingTable newRoutingTable = scaleIndexClusterStateBuilder.buildScaleUpRoutingTable(currentState, index);
            ClusterState tempState = ClusterState.builder(currentState).routingTable(newRoutingTable).build();

            ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(tempState.blocks());
            Metadata.Builder metadataBuilder = Metadata.builder(tempState.metadata());

            blocksBuilder.removeIndexBlockWithId(index, INDEX_SEARCH_ONLY_BLOCK_ID);
            IndexMetadata indexMetadata = tempState.metadata().index(index);
            Settings updatedSettings = Settings.builder()
                .put(indexMetadata.getSettings())
                .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)
                .build();
            metadataBuilder.put(
                IndexMetadata.builder(indexMetadata).settings(updatedSettings).settingsVersion(indexMetadata.getSettingsVersion() + 1)
            );

            return allocationService.reroute(
                ClusterState.builder(tempState).blocks(blocksBuilder).metadata(metadataBuilder).build(),
                "restore indexing shards"
            );
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            listener.onResponse(new AcknowledgedResponse(true));
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.error("Failed to execute cluster state update for scale up", e);
            listener.onFailure(e);
        }
    }

}
