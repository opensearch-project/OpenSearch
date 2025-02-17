package org.opensearch.action.admin.indices.searchonly;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
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
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.IndexId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class TransportSearchOnlyAction extends TransportClusterManagerNodeAction<SearchOnlyRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportSearchOnlyAction.class);
    public static final String NAME = SearchOnlyAction.NAME + "[s]";

    private final AllocationService allocationService;
    private final IndicesService indicesService;
    private final TransportService transportService;

    // Block ID and block for scale operations (IDs 20-29 reserved for scaling)
    public static final int INDEX_SEARCHONLY_BLOCK_ID = 20;
    public static final ClusterBlock INDEX_SEARCHONLY_BLOCK = new ClusterBlock(
        INDEX_SEARCHONLY_BLOCK_ID,
        "index scaled down",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE)
    );

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
            SearchOnlyAction.NAME,
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

        transportService.registerRequestHandler(
            NAME,
            ThreadPool.Names.SAME,
            NodeSearchOnlyRequest::new,
            (request, channel, task) -> handleShardSyncRequest(request, channel)
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(SearchOnlyRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request.indicesOptions(), request.indices());

        if (request.isScaleDown()) {
            submitScaleDownTask(concreteIndices, listener);
        } else {
            submitScaleUpTask(concreteIndices, state, listener);
        }
    }

    /**
     * Submits the scale-down update task: it first adds a temporary block to the indices and then initiates shard synchronization.
     */
    private void submitScaleDownTask(final String[] indices, final ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask(
            "add-block-index-to-scale " + Arrays.toString(indices),
            new ClusterStateUpdateTask(Priority.URGENT) {
                private final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    // Validate prerequisites for each index
                    for (String index : indices) {
                        IndexMetadata indexMetadata = currentState.metadata().index(index);
                        if (!validateScalePrerequisites(indexMetadata, index, listener, true)) {
                            return currentState;
                        }
                    }
                    return buildScaleDownState(currentState, indices, blockedIndices);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (oldState == newState) {
                        listener.onResponse(new AcknowledgedResponse(true));
                        return;
                    }
                    // Gather primary shard assignments
                    Map<ShardId, String> primaryShardsNodes = new HashMap<>();
                    for (String index : indices) {
                        IndexMetadata indexMetadata = newState.metadata().index(index);
                        if (indexMetadata != null) {
                            primaryShardsNodes.putAll(getPrimaryShardAssignments(indexMetadata, newState));
                        }
                    }
                    proceedWithScaleDown(indices, primaryShardsNodes, blockedIndices, listener);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    /**
     * Builds the new cluster state by adding a temporary scale-down block on each target index.
     */
    private ClusterState buildScaleDownState(ClusterState currentState, String[] indices, Map<Index, ClusterBlock> blockedIndices) {
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());

        for (String indexName : indices) {
            Index index = currentState.metadata().index(indexName).getIndex();
            ClusterBlock scaleBlock = createScaleDownBlock();
            blocksBuilder.addIndexBlock(indexName, scaleBlock);
            blockedIndices.put(index, scaleBlock);
        }
        return ClusterState.builder(currentState)
            .metadata(metadataBuilder)
            .blocks(blocksBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
    }

    /**
     * Returns a new temporary scale-down block.
     */
    private static ClusterBlock createScaleDownBlock() {
        return new ClusterBlock(
            INDEX_SEARCHONLY_BLOCK_ID,
            UUIDs.randomBase64UUID(),
            "index preparing to scale down",
            false,
            false,
            false,
            RestStatus.FORBIDDEN,
            EnumSet.of(ClusterBlockLevel.WRITE)
        );
    }

    /**
     * For each primary shard, groups the shard IDs by the node ID to which the primary is assigned
     */
    private Map<String, List<ShardId>> groupShardsByNode(Map<ShardId, String> primaryShardsNodes) {
        return primaryShardsNodes.entrySet()
            .stream()
            .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
    }

    /**
     * Sends shard sync requests to each node that holds a primary shard.
     */
    private void proceedWithScaleDown(
        String[] indices,
        Map<ShardId, String> primaryShardsNodes,
        Map<Index, ClusterBlock> blockedIndices,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (primaryShardsNodes.isEmpty()) {
            listener.onFailure(new IllegalStateException("No primary shards found for indices"));
            return;
        }
        final Map<String, List<ShardId>> nodeShardGroups = groupShardsByNode(primaryShardsNodes);

        final GroupedActionListener<NodeSearchOnlyResponse> groupedListener = new GroupedActionListener<>(
            ActionListener.wrap(
                responses -> handleNodeResponses(
                    responses,
                    ActionListener.wrap(searchOnlyResponse -> finalizeScaleDown(indices, blockedIndices, listener), listener::onFailure)
                ),
                listener::onFailure
            ),
            nodeShardGroups.size()
        );

        // Send a sync request to each node
        for (Map.Entry<String, List<ShardId>> entry : nodeShardGroups.entrySet()) {
            final String nodeId = entry.getKey();
            final List<ShardId> shards = entry.getValue();
            final DiscoveryNode targetNode = clusterService.state().nodes().get(nodeId);
            if (targetNode == null) {
                groupedListener.onFailure(new IllegalStateException("Node [" + nodeId + "] not found"));
                continue;
            }
            transportService.sendRequest(
                targetNode,
                NAME,
                new NodeSearchOnlyRequest(indices[0], shards),
                new TransportResponseHandler<NodeSearchOnlyResponse>() {
                    @Override
                    public NodeSearchOnlyResponse read(StreamInput in) throws IOException {
                        return new NodeSearchOnlyResponse(in);
                    }

                    @Override
                    public void handleResponse(NodeSearchOnlyResponse response) {
                        groupedListener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        groupedListener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                }
            );
        }
    }

    /**
     * Finalizes scale-down by updating the metadata and routing table:
     * removes the temporary block and adds a permanent search-only block.
     */
    private void finalizeScaleDown(
        String[] indices,
        Map<Index, ClusterBlock> blockedIndices,
        ActionListener<AcknowledgedResponse> listener
    ) {
        clusterService.submitStateUpdateTask("finalize-scale-down", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

                for (Map.Entry<Index, ClusterBlock> entry : blockedIndices.entrySet()) {
                    Index index = entry.getKey();
                    String indexName = index.getName();
                    // Remove temporary scale-down block
                    blocksBuilder.removeIndexBlockWithId(indexName, INDEX_SEARCHONLY_BLOCK_ID);

                    // Update index metadata: set search-only flag and update settings version
                    IndexMetadata indexMetadata = currentState.metadata().index(index);
                    if (indexMetadata != null) {
                        Settings updatedSettings = Settings.builder()
                            .put(indexMetadata.getSettings())
                            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true)
                            .build();
                        metadataBuilder.put(
                            IndexMetadata.builder(indexMetadata)
                                .settings(updatedSettings)
                                .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                        );
                    }
                    // Add permanent search-only block
                    blocksBuilder.addIndexBlock(indexName, INDEX_SEARCHONLY_BLOCK);
                }

                // Optionally update routing table to keep only search replicas
                for (String indexName : indices) {
                    IndexRoutingTable indexRoutingTable = currentState.routingTable().index(indexName);
                    if (indexRoutingTable == null) {
                        continue;
                    }
                    IndexRoutingTable.Builder indexBuilder = new IndexRoutingTable.Builder(indexRoutingTable.getIndex());
                    for (IndexShardRoutingTable shardTable : indexRoutingTable) {
                        IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shardTable.shardId());
                        for (ShardRouting shardRouting : shardTable) {
                            if (shardRouting.isSearchOnly()) {
                                shardBuilder.addShard(shardRouting);
                            }
                        }
                        indexBuilder.addIndexShard(shardBuilder.build());
                    }
                    routingTableBuilder.add(indexBuilder.build());
                }
                return ClusterState.builder(currentState)
                    .metadata(metadataBuilder)
                    .blocks(blocksBuilder)
                    .routingTable(routingTableBuilder.build())
                    .build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new AcknowledgedResponse(true));
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Handles an incoming shard sync request from another node.
     */
    private void handleShardSyncRequest(NodeSearchOnlyRequest request, TransportChannel channel) throws Exception {
        logger.info("Handling shard sync request");
        ClusterState state = clusterService.state();
        IndexMetadata indexMetadata = state.metadata().index(request.getIndex());
        if (indexMetadata == null) {
            throw new IllegalStateException("Index " + request.getIndex() + " not found");
        }
        IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
        if (indexService == null) {
            throw new IllegalStateException("IndexService not found for index " + request.getIndex());
        }

        List<ShardSearchOnlyResponse> shardResponses = new ArrayList<>();
        for (ShardId shardId : request.getShardIds()) {
            IndexShard shard = indexService.getShardOrNull(shardId.id());
            if (shard == null) {
                continue;
            }
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
            shardResponses.add(
                new ShardSearchOnlyResponse(shardId, shard.isSyncNeeded(), shard.translogStats().getUncommittedOperations())
            );
        }
        channel.sendResponse(new NodeSearchOnlyResponse(clusterService.localNode(), shardResponses));
    }

    /**
     * Aggregates node responses and verifies that no shard reports uncommitted operations or a pending sync.
     */
    private void handleNodeResponses(Collection<NodeSearchOnlyResponse> responses, ActionListener<SearchOnlyResponse> listener) {
        boolean hasUncommittedOps = false;
        boolean needsSync = false;
        List<String> failedShards = new ArrayList<>();

        for (NodeSearchOnlyResponse nodeResponse : responses) {
            for (ShardSearchOnlyResponse shardResponse : nodeResponse.getShardResponses()) {
                if (shardResponse.hasUncommittedOperations()) {
                    hasUncommittedOps = true;
                    failedShards.add(shardResponse.getShardId().toString());
                }
                if (shardResponse.needsSync()) {
                    needsSync = true;
                    failedShards.add(shardResponse.getShardId().toString());
                }
            }
        }

        if (hasUncommittedOps || needsSync) {
            String errorDetails = "Pre-scale sync failed for shards: "
                + String.join(", ", failedShards)
                + (hasUncommittedOps ? " - uncommitted operations remain" : "")
                + (needsSync ? " - sync needed" : "");
            listener.onFailure(new IllegalStateException(errorDetails));
        } else {
            listener.onResponse(new SearchOnlyResponse(responses));
        }
    }

    /**
     * Submits the scale-up update task that rebuilds the routing table and updates index metadata.
     */
    private void submitScaleUpTask(
        final String[] indices,
        final ClusterState currentState,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        // Validate prerequisites for scale-up
        for (String index : indices) {
            if (!validateScalePrerequisites(currentState.metadata().index(index), index, listener, false)) {
                return;
            }
        }
        clusterService.submitStateUpdateTask("scale-up-index", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                RoutingTable newRoutingTable = buildScaleUpRoutingTable(currentState, indices);
                ClusterState tempState = ClusterState.builder(currentState).routingTable(newRoutingTable).build();

                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(tempState.blocks());
                Metadata.Builder metadataBuilder = Metadata.builder(tempState.metadata());
                for (String indexName : indices) {
                    blocksBuilder.removeIndexBlockWithId(indexName, INDEX_SEARCHONLY_BLOCK_ID);
                    IndexMetadata indexMetadata = tempState.metadata().index(indexName);
                    Settings updatedSettings = Settings.builder()
                        .put(indexMetadata.getSettings())
                        .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)
                        .build();
                    metadataBuilder.put(
                        IndexMetadata.builder(indexMetadata)
                            .settings(updatedSettings)
                            .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                    );
                }
                // Reroute to allocate restored shards
                ClusterState reroutedState = allocationService.reroute(tempState, "restore indexing shards");
                return ClusterState.builder(tempState)
                    .blocks(blocksBuilder)
                    .metadata(metadataBuilder)
                    .routingTable(reroutedState.routingTable())
                    .build();
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
        });
    }

    /**
     * Rebuilds the routing table for scale-up: for each shard, only search replicas are kept and new primaries/replicas are added.
     */
    private RoutingTable buildScaleUpRoutingTable(ClusterState currentState, String[] indices) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        for (String index : indices) {
            IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
            if (indexRoutingTable == null) {
                continue;
            }
            IndexRoutingTable.Builder indexBuilder = new IndexRoutingTable.Builder(indexRoutingTable.getIndex());
            for (IndexShardRoutingTable shardTable : indexRoutingTable) {
                IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shardTable.shardId());
                // Retain existing search replicas
                for (ShardRouting shardRouting : shardTable) {
                    if (shardRouting.isSearchOnly()) {
                        shardBuilder.addShard(shardRouting);
                    }
                }
                // Create and add an unassigned primary with remote store recovery source
                RecoverySource.RemoteStoreRecoverySource remoteStoreRecoverySource = new RecoverySource.RemoteStoreRecoverySource(
                    UUID.randomUUID().toString(),
                    Version.CURRENT,
                    new IndexId(shardTable.shardId().getIndex().getName(), shardTable.shardId().getIndex().getUUID())
                );
                ShardRouting primaryShard = ShardRouting.newUnassigned(
                    shardTable.shardId(),
                    true,
                    remoteStoreRecoverySource,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "Restoring primary shard")
                );
                shardBuilder.addShard(primaryShard);

                // Add an unassigned replica
                ShardRouting replicaShard = ShardRouting.newUnassigned(
                    shardTable.shardId(),
                    false,
                    RecoverySource.PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "Restoring replica shard")
                );
                shardBuilder.addShard(replicaShard);
                indexBuilder.addIndexShard(shardBuilder.build());
            }
            routingTableBuilder.add(indexBuilder.build());
        }
        return routingTableBuilder.build();
    }

    /**
     * Validates that the given index meets the prerequisites for the scale operation.
     * For scale-down, checks that search replicas exist, remote store is enabled, and segment replication is used.
     * For scale-up, checks that the index is currently in search-only mode.
     */
    private boolean validateScalePrerequisites(
        IndexMetadata indexMetadata,
        String index,
        ActionListener<AcknowledgedResponse> listener,
        boolean isScaleDown
    ) {
        try {
            if (indexMetadata == null) {
                throw new IllegalArgumentException("Index [" + index + "] not found");
            }
            if (isScaleDown) {
                if (indexMetadata.getNumberOfSearchOnlyReplicas() == 0) {
                    throw new IllegalArgumentException("Cannot scale to zero without search replicas for index: " + index);
                }
                if (!indexMetadata.getSettings().getAsBoolean(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, false)) {
                    throw new IllegalArgumentException(
                        "To scale to zero, " + IndexMetadata.SETTING_REMOTE_STORE_ENABLED + " must be enabled for index: " + index
                    );
                }
                if (!ReplicationType.SEGMENT.toString().equals(indexMetadata.getSettings().get(IndexMetadata.SETTING_REPLICATION_TYPE))) {
                    throw new IllegalArgumentException("To scale to zero, segment replication must be enabled for index: " + index);
                }
            } else { // scale up
                if (!indexMetadata.getSettings().getAsBoolean(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)) {
                    throw new IllegalStateException("Index [" + index + "] is not in search-only mode");
                }
            }
            return true;
        } catch (Exception e) {
            listener.onFailure(e);
            return false;
        }
    }

    /**
     * Returns the primary shard node assignments for a given index.
     */
    private Map<ShardId, String> getPrimaryShardAssignments(IndexMetadata indexMetadata, ClusterState state) {
        Map<ShardId, String> assignments = new HashMap<>();
        for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
            ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
            ShardRouting primaryShard = state.routingTable().index(indexMetadata.getIndex().getName()).shard(i).primaryShard();
            if (primaryShard != null && primaryShard.assignedToNode()) {
                assignments.put(shardId, primaryShard.currentNodeId());
            }
        }
        return assignments;
    }

    @Override
    protected ClusterBlockException checkBlock(SearchOnlyRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(state, request.indicesOptions(), request.indices())
            );
    }
}
