package org.opensearch.action.admin.indices.scale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
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
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.inject.Inject;
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
import org.opensearch.cluster.routing.UnassignedInfo;
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
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class TransportPreScaleSyncAction extends TransportClusterManagerNodeAction<PreScaleSyncRequest, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(TransportPreScaleSyncAction.class);
    private final AllocationService allocationService;
    private final IndicesService indicesService;
    public static final String NAME = PreScaleSyncAction.NAME + "[s]";

    public static final int INDEX_SCALE_BLOCK_ID = 15;
    public static final ClusterBlock INDEX_SCALE_BLOCK = new ClusterBlock(
            INDEX_SCALE_BLOCK_ID,
            "index scaled down",
            false,
            false,
            false,
            RestStatus.FORBIDDEN,
            EnumSet.of(ClusterBlockLevel.WRITE)
    );

    @Inject
    public TransportPreScaleSyncAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            AllocationService allocationService,
            IndicesService indicesService
    ) {
        super(
                PreScaleSyncAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                PreScaleSyncRequest::new,
                indexNameExpressionResolver
        );
        this.allocationService = allocationService;
        this.indicesService = indicesService;

        transportService.registerRequestHandler(
                NAME,
                ThreadPool.Names.SAME,
                NodePreScaleSyncRequest::new,
                (request, channel, task) -> handleShardSyncRequest(request, channel)
        );
    }

    private static ClusterBlock createScaleBlock() {
        return new ClusterBlock(
                INDEX_SCALE_BLOCK_ID,
                UUIDs.randomBase64UUID(),
                "index preparing to scale down",
                false,
                false,
                false,
                RestStatus.FORBIDDEN,
                EnumSet.of(ClusterBlockLevel.WRITE)
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
    protected void clusterManagerOperation(
            PreScaleSyncRequest request,
            ClusterState state,
            ActionListener<AcknowledgedResponse> listener
    ) {
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
                state,
                request.indicesOptions(),
                request.indices()
        );

        if (request.isScaleDown()) {
            addBlockAndScaleDown(concreteIndices, state, listener);
        } else {
            scaleUp(concreteIndices, state, listener);
        }
    }

    private void addBlockAndScaleDown(
            final String[] indices,
            final ClusterState currentState,
            final ActionListener<AcknowledgedResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
                "add-block-index-to-scale " + Arrays.toString(indices),
                new ClusterStateUpdateTask(Priority.URGENT) {
                    private final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

                    @Override
                    public ClusterState execute(final ClusterState currentState) {
                        for (String index : indices) {
                            IndexMetadata indexMetadata = currentState.metadata().index(index);
                            if (!validateScaleDownPrerequisites(indexMetadata, index, listener)) {
                                return currentState;
                            }
                        }

                        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());
                        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());

                        for (String indexName : indices) {
                            Index index = currentState.metadata().index(indexName).getIndex();
                            ClusterBlock scaleBlock = createScaleBlock();
                            blocks.addIndexBlock(indexName, scaleBlock);
                            blockedIndices.put(index, scaleBlock);
                        }

                        return ClusterState.builder(currentState)
                                .metadata(metadata)
                                .blocks(blocks)
                                .routingTable(routingTable.build())
                                .build();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        if (oldState == newState) {
                            listener.onResponse(new AcknowledgedResponse(true));
                            return;
                        }

                        Map<ShardId, String> primaryShardsNodes = new HashMap<>();
                        for (String index : indices) {
                            IndexMetadata indexMetadata = newState.metadata().index(index);
                            if (indexMetadata != null) {
                                primaryShardsNodes.putAll(getPrimaryShardNodeAssignments(indexMetadata, newState));
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

    private void handleShardSyncRequest(NodePreScaleSyncRequest request, TransportChannel channel) throws Exception {
        logger.info("Handling shard sync request");
        final ClusterState state = clusterService.state();
        final IndexMetadata indexMetadata = state.metadata().index(request.getIndex());
        if (indexMetadata == null) {
            throw new IllegalStateException("Index " + request.getIndex() + " not found");
        }

        IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
        if (indexService == null) {
            throw new IllegalStateException("IndexService not found for index " + request.getIndex());
        }

        List<ShardPreScaleSyncResponse> shardResponses = new ArrayList<>();
        for (ShardId shardId : request.getShardIds()) {
            IndexShard shard = indexService.getShardOrNull(shardId.id());
            if (shard == null) continue;

            logger.info("Doing final Sync before closing shard");
            shard.sync();
            logger.info("Doing final Flush before closing shard");
            shard.flush(new FlushRequest().force(true).waitIfOngoing(true));

            if (shard.translogStats().getUncommittedOperations() > 0) {
                logger.info(
                        "Translog has {} uncommitted operations before closing shard [{}]",
                        shard.translogStats().getUncommittedOperations(),
                        shard.shardId()
                );
                throw new IllegalStateException(
                        String.format("Shard [%s] still has %d uncommitted operations after flush. Please wait and retry the scale down operation.",
                                shard.shardId(),
                                shard.translogStats().getUncommittedOperations())
                );
            }

            shard.waitForRemoteStoreSync();

            shardResponses.add(
                    new ShardPreScaleSyncResponse(
                            shardId,
                            shard.isSyncNeeded(),
                            shard.translogStats().getUncommittedOperations()
                    )
            );
        }

        channel.sendResponse(new NodePreScaleSyncResponse(clusterService.localNode(), shardResponses));
    }

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

        Map<String, List<ShardId>> nodeShardGroups = primaryShardsNodes.entrySet()
                .stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())
                ));

        final GroupedActionListener<NodePreScaleSyncResponse> groupedListener = new GroupedActionListener<>(
                ActionListener.wrap(
                        responses -> handleNodeResponses(
                                responses,
                                ActionListener.wrap(
                                        preScaleSyncResponse -> finalizeScaleDown(indices, blockedIndices, listener),
                                        listener::onFailure
                                )
                        ),
                        listener::onFailure
                ),
                nodeShardGroups.size()
        );

        for (Map.Entry<String, List<ShardId>> nodeShards : nodeShardGroups.entrySet()) {
            final String nodeId = nodeShards.getKey();
            final List<ShardId> shards = nodeShards.getValue();

            final DiscoveryNode targetNode = clusterService.state().nodes().get(nodeId);
            if (targetNode == null) {
                groupedListener.onFailure(new IllegalStateException("Node [" + nodeId + "] not found"));
                continue;
            }

            transportService.sendRequest(
                    targetNode,
                    NAME,
                    new NodePreScaleSyncRequest(indices[0], shards),
                    new TransportResponseHandler<NodePreScaleSyncResponse>() {
                        @Override
                        public NodePreScaleSyncResponse read(StreamInput in) throws IOException {
                            return new NodePreScaleSyncResponse(in);
                        }

                        @Override
                        public void handleResponse(NodePreScaleSyncResponse response) {
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

    private void finalizeScaleDown(
            String[] indices,
            Map<Index, ClusterBlock> blockedIndices,
            ActionListener<AcknowledgedResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
                "finalize-scale-down",
                new ClusterStateUpdateTask(Priority.URGENT) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                        RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());

                        for (Map.Entry<Index, ClusterBlock> entry : blockedIndices.entrySet()) {
                            Index index = entry.getKey();
                            blocks.removeIndexBlockWithId(index.getName(), INDEX_SCALE_BLOCK_ID);
                            blocks.addIndexBlock(index.getName(), INDEX_SCALE_BLOCK);
                        }

                        for (String index : indices) {
                            IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
                            if (indexRoutingTable == null) continue;

                            IndexRoutingTable.Builder indexBuilder = new IndexRoutingTable.Builder(
                                    indexRoutingTable.getIndex()
                            );

                            // Keep only search replicas in the routing table
                            for (IndexShardRoutingTable shardTable : indexRoutingTable) {
                                IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(
                                        shardTable.shardId()
                                );

                                for (ShardRouting shardRouting : shardTable) {
                                    if (shardRouting.isSearchOnly()) {
                                        shardBuilder.addShard(shardRouting);
                                    }
                                }

                                indexBuilder.addIndexShard(shardBuilder.build());
                            }

                            routingTable.add(indexBuilder.build());
                        }

                        return ClusterState.builder(currentState)
                                .blocks(blocks)
                                .routingTable(routingTable.build())
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
                }
        );
    }

    private void handleNodeResponses(
            Collection<NodePreScaleSyncResponse> responses,
            ActionListener<PreScaleSyncResponse> listener
    ) {
        boolean hasUncommittedOps = false;
        boolean needsSync = false;
        List<String> failedShards = new ArrayList<>();

        for (NodePreScaleSyncResponse nodeResponse : responses) {
            for (ShardPreScaleSyncResponse shardResponse : nodeResponse.getShardResponses()) {
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
            listener.onFailure(new IllegalStateException(
                    "Pre-scale sync failed for shards: " + String.join(", ", failedShards) +
                            (hasUncommittedOps ? " - uncommitted operations remain" : "") +
                            (needsSync ? " - sync needed" : "")
            ));
            return;
        }

        listener.onResponse(new PreScaleSyncResponse(responses));
    }

    private void scaleUp(
            final String[] indices,
            final ClusterState currentState,
            final ActionListener<AcknowledgedResponse> listener
    ) {
        // 1. Update routing table
        clusterService.submitStateUpdateTask(
                "scale-up-index",
                new ClusterStateUpdateTask() {
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());

                        // For each index, modify its routing table
                        for (String index : indices) {
                            IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
                            if (indexRoutingTable == null) continue;

                            // Build new routing table
                            IndexRoutingTable.Builder indexBuilder = new IndexRoutingTable.Builder(indexRoutingTable.getIndex());

                            for (IndexShardRoutingTable shardTable : indexRoutingTable) {
                                IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shardTable.shardId());

                                // Keep existing search replicas
                                for (ShardRouting shardRouting : shardTable) {
                                    if (shardRouting.isSearchOnly()) {
                                        shardBuilder.addShard(shardRouting);
                                    }
                                }

                                // Create recovery source for primary
                                RecoverySource.RemoteStoreRecoverySource remoteStoreRecoverySource = new RecoverySource.RemoteStoreRecoverySource(
                                        UUID.randomUUID().toString(),
                                        Version.CURRENT,
                                        new IndexId(
                                                shardTable.shardId().getIndex().getName(),
                                                shardTable.shardId().getIndex().getUUID()
                                        )
                                );

                                // Add unassigned primary
                                ShardRouting primaryShard = ShardRouting.newUnassigned(
                                        shardTable.shardId(),
                                        true,
                                        remoteStoreRecoverySource,
                                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "Restoring primary shard")
                                );
                                shardBuilder.addShard(primaryShard);

                                // Add unassigned replica
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

                        ClusterState tempState = ClusterState.builder(currentState)
                                .routingTable(routingTableBuilder.build())
                                .build();

                        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(tempState.blocks());
                        for (String indexName : indices) {
                            blocks.removeIndexBlockWithId(indexName, INDEX_SCALE_BLOCK_ID);
                        }
                        // Perform reroute to allocate restored shards
                        return ClusterState.builder(tempState)
                                .blocks(blocks)
                                .routingTable(allocationService.reroute(tempState, "restore indexing shards").routingTable())
                                .build();

                    }

                    public void onFailure(String source, Exception e) {
                        logger.error("Failed to execute cluster state update for scale up", e);
                        listener.onFailure(e);
                    }

                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(new AcknowledgedResponse(true));
                    }
                }
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PreScaleSyncRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(state, request.indicesOptions(), request.indices()));
    }

    private boolean validateScaleDownPrerequisites(
            IndexMetadata indexMetadata,
            String index,
            ActionListener<AcknowledgedResponse> listener
    ) {
        // Validate search replicas exist
        if (indexMetadata.getNumberOfSearchOnlyReplicas() == 0) {
            listener.onFailure(new IllegalArgumentException(
                    "Cannot scale to zero without search replicas for index: " + index
            ));
            return false;
        }

        // Validate remote store is enabled
        if (!indexMetadata.getSettings().getAsBoolean(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, false)) {
            listener.onFailure(new IllegalArgumentException(
                    "To scale to zero, " + IndexMetadata.SETTING_REMOTE_STORE_ENABLED +
                            " must be enabled for index: " + index
            ));
            return false;
        }

        // Validate segment replication
        if (!ReplicationType.SEGMENT.toString().equals(
                indexMetadata.getSettings().get(IndexMetadata.SETTING_REPLICATION_TYPE))
        ) {
            listener.onFailure(new IllegalArgumentException(
                    "To scale to zero, segment replication must be enabled for index: " + index
            ));
            return false;
        }
        return true;
    }

    private Map<ShardId, String> getPrimaryShardNodeAssignments(IndexMetadata indexMetadata, ClusterState state) {
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
}
