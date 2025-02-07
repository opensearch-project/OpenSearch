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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Transport action implementation for search-only scale operations.
 * <p>
 * This class coordinates the entire process of scaling indices up or down between normal
 * and search-only modes. It manages the multi-step process including:
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
public class TransportSearchOnlyAction extends TransportClusterManagerNodeAction<SearchOnlyRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportSearchOnlyAction.class);
    /** Transport action name for shard sync requests */
    public static final String NAME = SearchOnlyScaleAction.NAME + "[s]";

    private final AllocationService allocationService;
    private final IndicesService indicesService;
    private final TransportService transportService;

    private final ScaleOperationValidator validator;
    private final SearchOnlyClusterStateBuilder searchOnlyClusterStateBuilder;
    private final SearchOnlyShardSyncManager searchOnlyShardSyncManager;

    // Block ID and block for scale operations (IDs 20-29 reserved for scaling)
    public static final int INDEX_SEARCHONLY_BLOCK_ID = 20;

    /**
     * Permanent cluster block applied to indices in search-only mode.
     * <p>
     * This block prevents write operations to the index while allowing read operations.
     */
    public static final ClusterBlock INDEX_SEARCHONLY_BLOCK = new ClusterBlock(
        INDEX_SEARCHONLY_BLOCK_ID,
        "index scaled down",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE)
    );

    /**
     * Constructs a new TransportSearchOnlyAction.
     *
     * @param transportService          the transport service for network communication
     * @param clusterService            the cluster service for accessing cluster state
     * @param threadPool                the thread pool for executing operations
     * @param actionFilters             filters for action requests
     * @param indexNameExpressionResolver resolver for index names and expressions
     * @param allocationService         service for shard allocation decisions
     * @param indicesService            service for accessing index shards
     */
    @Inject
    public TransportSearchOnlyAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        super(
            SearchOnlyScaleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SearchOnlyRequest::new,
            indexNameExpressionResolver
        );
        this.allocationService = allocationService;
        this.indicesService = indicesService;
        this.transportService = transportService;
        this.validator = new ScaleOperationValidator();
        this.searchOnlyClusterStateBuilder = new SearchOnlyClusterStateBuilder();
        this.searchOnlyShardSyncManager = new SearchOnlyShardSyncManager(clusterService, transportService, NAME);

        transportService.registerRequestHandler(
            NAME,
            ThreadPool.Names.SAME,
            NodeSearchOnlyRequest::new,
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
     * @param request the search-only scale request
     * @param state   the current cluster state
     * @param listener the listener to notify with the operation result
     */
    @Override
    protected void clusterManagerOperation(SearchOnlyRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
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
        searchOnlyShardSyncManager.sendShardSyncRequests(
            index,
            primaryShardsNodes,
            ActionListener.wrap(responses -> handleShardSyncResponses(responses, index, listener), listener::onFailure)
        );
    }

    private void handleShardSyncResponses(
        Collection<NodeSearchOnlyResponse> responses,
        String index,
        ActionListener<AcknowledgedResponse> listener
    ) {
        searchOnlyShardSyncManager.validateNodeResponses(
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
    private void handleShardSyncRequest(NodeSearchOnlyRequest request, TransportChannel channel) throws Exception {
        logger.info("Handling shard sync request for index [{}]", request.getIndex());
        ClusterState state = clusterService.state();

        IndexMetadata indexMetadata = state.metadata().index(request.getIndex());
        if (indexMetadata == null) {
            throw new IllegalStateException("Index " + request.getIndex() + " not found");
        }

        IndexService indexService = getIndexService(indexMetadata);
        List<ShardSearchOnlyResponse> shardResponses = syncShards(indexService, request.getShardIds());

        channel.sendResponse(new NodeSearchOnlyResponse(clusterService.localNode(), shardResponses));
    }

    private IndexService getIndexService(IndexMetadata indexMetadata) {
        IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
        if (indexService == null) {
            throw new IllegalStateException("IndexService not found for index " + indexMetadata.getIndex().getName());
        }
        return indexService;
    }

    private List<ShardSearchOnlyResponse> syncShards(IndexService indexService, List<ShardId> shardIds) throws Exception {
        List<ShardSearchOnlyResponse> shardResponses = new ArrayList<>();

        for (ShardId shardId : shardIds) {
            IndexShard shard = indexService.getShardOrNull(shardId.id());
            if (shard == null) {
                continue;
            }

            shardResponses.add(syncSingleShard(shard));
        }

        return shardResponses;
    }

    private ShardSearchOnlyResponse syncSingleShard(IndexShard shard) throws Exception {
        logger.info("Performing final sync and flush for shard {}", shard.shardId());
        shard.sync();
        shard.flush(new FlushRequest().force(true).waitIfOngoing(true));

        if (shard.translogStats().getUncommittedOperations() > 0) {
            String errorMsg = String.format(
                Locale.ROOT,
                "Shard [%s] still has %d uncommitted operations after flush. Please wait and retry the scale down operation.",
                shard.shardId(),
                shard.translogStats().getUncommittedOperations()
            );
            throw new IllegalStateException(errorMsg);
        }

        shard.waitForRemoteStoreSync();
        return new ShardSearchOnlyResponse(shard.shardId(), shard.isSyncNeeded(), shard.translogStats().getUncommittedOperations());
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
    protected ClusterBlockException checkBlock(SearchOnlyRequest request, ClusterState state) {
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
    private class AddBlockClusterStateUpdateTask extends ClusterStateUpdateTask {
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
                return searchOnlyClusterStateBuilder.buildScaleDownState(currentState, index, blockedIndices);
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
                Map<ShardId, String> primaryShardsNodes = searchOnlyShardSyncManager.getPrimaryShardAssignments(indexMetadata, newState);
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
    private class FinalizeScaleDownTask extends ClusterStateUpdateTask {
        private final String index;
        private final ActionListener<AcknowledgedResponse> listener;

        FinalizeScaleDownTask(String index, ActionListener<AcknowledgedResponse> listener) {
            super(Priority.URGENT);
            this.index = index;
            this.listener = listener;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            return searchOnlyClusterStateBuilder.buildFinalScaleDownState(currentState, index);
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
            RoutingTable newRoutingTable = searchOnlyClusterStateBuilder.buildScaleUpRoutingTable(currentState, index);
            ClusterState tempState = ClusterState.builder(currentState).routingTable(newRoutingTable).build();

            ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(tempState.blocks());
            Metadata.Builder metadataBuilder = Metadata.builder(tempState.metadata());

            blocksBuilder.removeIndexBlockWithId(index, INDEX_SEARCHONLY_BLOCK_ID);
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
