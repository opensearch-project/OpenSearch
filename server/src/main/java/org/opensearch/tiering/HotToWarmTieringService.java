/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.opensearch.action.tiering.HotToWarmTieringRequestContext;
import org.opensearch.action.tiering.HotToWarmTieringResponse;
import org.opensearch.action.tiering.TieringIndexRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.index.IndexModule.INDEX_STORE_LOCALITY_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.tiering.TieringServiceValidator.validateHotToWarm;

/**
 * Service responsible for tiering indices from hot to warm
 * @opensearch.experimental
 */
public class HotToWarmTieringService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(HotToWarmTieringService.class);

    protected final ClusterService clusterService;

    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    protected final AllocationService allocationService;

    protected final ClusterInfoService clusterInfoService;
    protected final DiskThresholdSettings diskThresholdSettings;
    public static final Setting<Long> HOT_TO_WARM_START_TIME_SETTING = Setting.longSetting(
        "index.tiering.hot_to_warm.start_time",
        System.currentTimeMillis(),
        0,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );

    public static final Setting<Long> HOT_TO_WARM_END_TIME_SETTING = Setting.longSetting(
        "index.tiering.hot_to_warm.end_time",
        0,
        0,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );
    private Map<Index, Map<ShardId, ShardTieringStatus>> indexShardTieringStatus = new HashMap<>();
    private Map<String, String> indexToRequestUuid = new HashMap<>();
    private Map<String, HotToWarmTieringRequestContext> requestUuidToRequestContext = new HashMap<>();

    @Inject
    public HotToWarmTieringService(
        Settings settings,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService,
        ClusterInfoService clusterInfoService
    ) {
        super();
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.allocationService = allocationService;
        this.clusterInfoService = clusterInfoService;
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterService.getClusterSettings());

        if (DiscoveryNode.isClusterManagerNode(settings) && FeatureFlags.isEnabled(FeatureFlags.TIERED_REMOTE_INDEX)) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO add handling for master switch, dangling indices, master reload
        if (event.routingTableChanged()) {
            updateIndexShardStatus(event.state());
        }
    }

    public void completeRequestLevelTiering(Index index) {
        String requestUuid = indexToRequestUuid.get(index.getName());
        if (requestUuid == null) {
            logger.debug("Tiering for the index [{}] is already marked as completed", index.getName());
            return;
        }
        HotToWarmTieringRequestContext requestContext = requestUuidToRequestContext.get(requestUuid);
        assert requestContext != null : "requestContext cannot be null for request uuid " + requestUuid;
        if (requestContext.isRequestProcessingComplete()) {
            logger.info("Tiering is completed for the request [{}]", requestContext);
            for (Index indexToRemove : requestContext.getAcceptedIndices()) {
                indexToRequestUuid.remove(indexToRemove.getName());
            }
            requestUuidToRequestContext.remove(requestUuid);
            if (requestContext.getRequest().waitForCompletion()) {
                requestContext.getListener().onResponse(requestContext.constructResponse());
            }
        }
    }

    private void updateIndexShardStatus(ClusterState clusterState) {
        List<ShardRouting> routingTable;
        Set<Index> relocationCompletedIndices = new HashSet<>();
        Set<Index> failedIndices = new HashSet<>();
        for (Index index : indexShardTieringStatus.keySet()) {
            try {
                // Ensure index is not deleted
                routingTable = clusterState.routingTable().allShards(index.getName());
            } catch (IndexNotFoundException ex) {
                // Index already deleted nothing to do
                logger.warn("Index [{}] deleted before hot to warm relocation finished", index.getName());
                HotToWarmTieringRequestContext requestContext = requestUuidToRequestContext.get(indexToRequestUuid.get(index.getName()));
                requestContext.addToNotFound(index.getName());
                requestContext.removeFromInProgress(index);
                indexShardTieringStatus.remove(index);
                completeRequestLevelTiering(index);
                continue;
            }

            Map<ShardId, ShardTieringStatus> shardTieringStatusMap = indexShardTieringStatus.getOrDefault(index, new HashMap<>());
            List<ShardId> processingShards = shardTieringStatusMap.keySet()
                .stream()
                .filter(
                    shardId -> shardTieringStatusMap.get(shardId).state() == State.INIT
                        || shardTieringStatusMap.get(shardId).state() == State.PROCESSING
                )
                .collect(Collectors.toList());
            if (processingShards.isEmpty()) {
                // No shards are in processing state, nothing to do
                // This means that tiering for the index is completed - either failed or successful
                continue;
            }
            boolean relocationCompleted = true;
            for (ShardRouting shard : routingTable) {
                if (shardTieringStatusMap.get(shard.shardId()) != null
                    && (State.SUCCESSFUL.equals(shardTieringStatusMap.get(shard.shardId()).state())
                        || State.FAILED.equals(shardTieringStatusMap.get(shard.shardId()).state()))) {
                    continue;
                }
                State tieringState;
                String reason = null;
                boolean isShardFoundOnSearchNode = clusterState.getNodes().get(shard.currentNodeId()).isSearchNode();
                boolean isShardRelocatingToSearchNode = clusterState.getNodes().get(shard.relocatingNodeId()).isSearchNode();

                if (shard.started() && isShardFoundOnSearchNode) {
                    tieringState = State.SUCCESSFUL;
                } else if (shard.unassigned()) {
                    tieringState = State.FAILED;
                    relocationCompleted = false;
                    failedIndices.add(index);
                    reason = "Shard is unassigned due to " + shard.unassignedInfo().getReason();
                } else if ((shard.initializing() && !isShardFoundOnSearchNode) || (shard.relocating() && !isShardRelocatingToSearchNode)) {
                    tieringState = State.FAILED;
                    relocationCompleted = false;
                    failedIndices.add(index);
                    reason = "Shard with current state: "
                        + shard.state().toString()
                        + "is neither allocated nor relocating to the search node, "
                        + "current node: "
                        + shard.currentNodeId()
                        + ", relocating node: "
                        + shard.relocatingNodeId();
                } else {
                    tieringState = State.PROCESSING;
                    relocationCompleted = false;
                }
                shardTieringStatusMap.put(
                    shard.shardId(),
                    new ShardTieringStatus(shard.currentNodeId(), shard.state(), tieringState, reason)
                );
            }
            indexShardTieringStatus.put(index, shardTieringStatusMap);
            if (relocationCompleted) {
                logger.info("Hot to warm relocation completed for index [{}]", index.getName());
                relocationCompletedIndices.add(index);
            }
        }
        if (!relocationCompletedIndices.isEmpty()) {
            processSuccessfullyTieredIndices(relocationCompletedIndices);
        }
        if (!failedIndices.isEmpty()) {
            processFailedIndices(failedIndices);
        }
    }

    private void processFailedIndices(Set<Index> indices) {
        clusterService.submitStateUpdateTask("process hot to warm tiering for failed indices", new ClusterStateUpdateTask(Priority.URGENT) {

            @Override
            public ClusterState execute(ClusterState currentState) {
                final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());

                for (Index index : indices) {
                    final IndexMetadata indexMetadata = metadataBuilder.get(index.getName());
                    Settings.Builder indexSettingsBuilder = Settings.builder().put(indexMetadata.getSettings());

                    // update tiering settings here
                    indexSettingsBuilder.put(INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.FULL);
                    indexSettingsBuilder.put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT);
                    indexSettingsBuilder.put(HOT_TO_WARM_END_TIME_SETTING.getKey(), System.currentTimeMillis());

                    // Update number of replicas to 1 in case the number of replicas is lesser than 1
                    if (Integer.parseInt(metadataBuilder.getSafe(index).getSettings().get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey())) < 1) {
                        final String[] indices = new String[] { index.getName() };
                        routingTableBuilder.updateNumberOfReplicas(1, indices);
                        metadataBuilder.updateNumberOfReplicas(1, indices);
                    }

                    // Update index settings version
                    final IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata).settings(indexSettingsBuilder);
                    builder.settingsVersion(1 + builder.settingsVersion());
                    metadataBuilder.put(builder);
                }

                final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());

                ClusterState updatedState = ClusterState.builder(currentState)
                    .metadata(metadataBuilder)
                    .routingTable(routingTableBuilder.build())
                    .blocks(blocks)
                    .build();

                // now, reroute to trigger shard relocation for shards to go back to hot nodes
                updatedState = allocationService.reroute(updatedState, "hot to warm revert tiering");

                return updatedState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage("failed to complete hot to warm tiering for indices " + "[{}]", indices),
                    e
                );
            }

            @Override
            public void onNoLongerClusterManager(String source) {
                this.onFailure(source, new NotClusterManagerException("no longer cluster manager. source: [" + source + "]"));
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("Cluster state updated for source " + source);
                for (Index index : indices) {
                    HotToWarmTieringRequestContext requestContext = requestUuidToRequestContext.get(
                        indexToRequestUuid.get(index.getName())
                    );
                    requestContext.addToFailed(index.getName());
                    requestContext.removeFromInProgress(index);
                    completeRequestLevelTiering(index);
                }
            }
        });
    }

    private void processSuccessfullyTieredIndices(Set<Index> indices) {
        clusterService.submitStateUpdateTask("complete hot to warm tiering", new ClusterStateUpdateTask(Priority.URGENT) {

            @Override
            public ClusterState execute(ClusterState currentState) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

                for (Index index : indices) {
                    final IndexMetadata indexMetadata = metadataBuilder.get(index.getName());
                    Settings.Builder indexSettingsBuilder = Settings.builder().put(indexMetadata.getSettings());
                    // put/update tiering settings here
                    indexSettingsBuilder.put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM);
                    indexSettingsBuilder.put(HOT_TO_WARM_END_TIME_SETTING.getKey(), System.currentTimeMillis());

                    // Update index settings version
                    final IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata).settings(indexSettingsBuilder);
                    builder.settingsVersion(1 + builder.settingsVersion());
                    metadataBuilder.put(builder);
                }

                final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());

                ClusterState updatedState = ClusterState.builder(currentState)
                    .metadata(metadataBuilder)
                    .routingTable(routingTableBuilder.build())
                    .blocks(blocks)
                    .build();

                return updatedState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage("failed to complete hot to warm tiering for indices " + "[{}]", indices),
                    e
                );
            }

            @Override
            public void onNoLongerClusterManager(String source) {
                this.onFailure(source, new NotClusterManagerException("no longer cluster manager. source: [" + source + "]"));
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("Cluster state updated for source " + source);
                for (Index index : indices) {
                    indexShardTieringStatus.remove(index);
                    HotToWarmTieringRequestContext requestContext = requestUuidToRequestContext.get(
                        indexToRequestUuid.get(index.getName())
                    );
                    requestContext.addToSuccessful(index.getName());
                    requestContext.removeFromInProgress(index);
                    completeRequestLevelTiering(index);
                }
            }
        });
    }

    /**
     * Tier indices from hot to warm
     * @param request - tiering request
     * @param listener - call back listener
     */
    public void tier(final TieringIndexRequest request, final ActionListener<HotToWarmTieringResponse> listener) {
        clusterService.submitStateUpdateTask("start hot to warm tiering", new ClusterStateUpdateTask(Priority.URGENT) {
            final HotToWarmTieringRequestContext hotToWarmTieringRequestContext = new HotToWarmTieringRequestContext(listener, request);

            @Override
            public ClusterState execute(ClusterState currentState) {
                List<Index> concreteIndices = new ArrayList<>();
                for (String index : request.indices()) {
                    Index[] resolvedIndices = null;
                    try {
                        resolvedIndices = indexNameExpressionResolver.concreteIndices(currentState, request.indicesOptions(), index);
                    } catch (IndexNotFoundException e) {
                        hotToWarmTieringRequestContext.addToNotFound(e.getIndex().getName());
                        logger.debug("Index [{}] not found: {}", index, e);
                    }
                    if (resolvedIndices != null) {
                        concreteIndices.addAll(Arrays.asList(resolvedIndices));
                    }
                }
                String requestUuid = hotToWarmTieringRequestContext.getRequestUuid();
                HotToWarmTieringRequestContext validationResult = validateHotToWarm(
                    currentState,
                    concreteIndices.toArray(Index.EMPTY_ARRAY),
                    clusterInfoService.getClusterInfo(),
                    diskThresholdSettings
                );
                hotToWarmTieringRequestContext.setAcceptedIndices(validationResult.getAcceptedIndices());
                hotToWarmTieringRequestContext.setRejectedIndices(validationResult.getRejectedIndices());
                hotToWarmTieringRequestContext.setInProgressIndices(validationResult.getAcceptedIndices());
                requestUuidToRequestContext.put(requestUuid, hotToWarmTieringRequestContext);
                final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

                for (Index index : validationResult.getAcceptedIndices()) {
                    final IndexMetadata indexMetadata = metadataBuilder.get(index.getName());
                    Settings.Builder indexSettingsBuilder = Settings.builder().put(indexMetadata.getSettings());
                    // put additional settings here
                    indexSettingsBuilder.put(INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL);
                    indexSettingsBuilder.put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT_TO_WARM);
                    indexSettingsBuilder.put(HOT_TO_WARM_START_TIME_SETTING.getKey(), System.currentTimeMillis());
                    indexSettingsBuilder.put(HOT_TO_WARM_END_TIME_SETTING.getKey(), -1);

                    // Update number of replicas to 1 in case the number of replicas is greater than 1
                    if (Integer.parseInt(metadataBuilder.getSafe(index).getSettings().get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey())) > 1) {
                        final String[] indices = new String[] { index.getName() };
                        routingTableBuilder.updateNumberOfReplicas(1, indices);
                        metadataBuilder.updateNumberOfReplicas(1, indices);
                    }

                    // Update index settings version
                    final IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata).settings(indexSettingsBuilder);
                    builder.settingsVersion(1 + builder.settingsVersion());
                    metadataBuilder.put(builder);
                    Map<ShardId, ShardTieringStatus> shardTieringStatus = new HashMap<>();
                    for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
                        ShardId shardId = new ShardId(indexMetadata.getIndex(), shard);
                        shardTieringStatus.put(shardId, new ShardTieringStatus(currentState.nodes().getLocalNodeId(), null));
                    }
                    indexShardTieringStatus.put(index, shardTieringStatus);
                    indexToRequestUuid.put(index.getName(), requestUuid);
                }

                final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());

                ClusterState updatedState = ClusterState.builder(currentState)
                    .metadata(metadataBuilder)
                    .routingTable(routingTableBuilder.build())
                    .blocks(blocks)
                    .build();

                // now, reroute to trigger shard relocation for the dedicated case
                updatedState = allocationService.reroute(updatedState, "hot to warm tiering");

                return updatedState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "failed to start warm tiering for indices " + "[{}]",
                        (Object) request.indices()
                    ),
                    e
                );
                listener.onFailure(e);
            }

            @Override
            public void onNoLongerClusterManager(String source) {
                this.onFailure(source, new NotClusterManagerException("no longer cluster manager. source: [" + source + "]"));
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("Cluster state updated for source " + source);
                if (!request.waitForCompletion()) {
                    listener.onResponse(hotToWarmTieringRequestContext.constructResponse());
                }
            }

            @Override
            public TimeValue timeout() {
                return request.clusterManagerNodeTimeout();
            }
        });
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}

    /**
     * Represents status of a tiering shard
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class ShardTieringStatus {
        private State state;
        private ShardRoutingState shardRoutingState;
        private String nodeId;
        private String reason;

        private ShardTieringStatus() {}

        /**
         * Constructs a new shard tiering status in initializing state on the given node
         *
         * @param nodeId node id
         */
        public ShardTieringStatus(String nodeId) {
            this(nodeId, null);
        }

        /**
         * Constructs a new shard tiering status in with specified state on the given node
         *
         * @param nodeId node id
         * @param shardRoutingState  shard state
         */
        public ShardTieringStatus(String nodeId, ShardRoutingState shardRoutingState) {
            this(nodeId, shardRoutingState, State.INIT, null);
        }

        /**
         * Constructs a new shard tiering status in with specified state on the given node with specified failure reason
         *
         * @param nodeId node id
         * @param state  shard state
         * @param reason failure reason
         */
        public ShardTieringStatus(String nodeId, ShardRoutingState shardRoutingState, State state, String reason) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
            this.shardRoutingState = shardRoutingState;
        }

        /**
         * Returns current state
         *
         * @return current state
         */
        public State state() {
            return state;
        }

        /**
         * Returns node id of the node where shared is getting tiered
         *
         * @return node id
         */
        public String nodeId() {
            return nodeId;
        }

        /**
         * Returns failure reason
         *
         * @return failure reason
         */
        public String reason() {
            return reason;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ShardTieringStatus status = (ShardTieringStatus) o;
            return state == status.state && Objects.equals(nodeId, status.nodeId) && Objects.equals(reason, status.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, nodeId, reason);
        }
    }

    /**
     * Tiering state
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public enum State {
        /**
         * Initializing state
         */
        INIT((byte) 0),
        /**
         * Processing state
         */
        PROCESSING((byte) 1),
        /**
         * Tiering finished successfully
         */
        SUCCESSFUL((byte) 2),
        /**
         * Tiering failed
         */
        FAILED((byte) 3);

        private final byte value;

        /**
         * Constructs new state
         *
         * @param value state code
         */
        State(byte value) {
            this.value = value;
        }

        /**
         * Returns state code
         *
         * @return state code
         */
        public byte value() {
            return value;
        }

        /**
         * Returns true if tiering completed (either successfully or with failure)
         *
         * @return true if tiering completed
         */
        public boolean completed() {
            return this == SUCCESSFUL || this == FAILED;
        }

        /**
         * Returns state corresponding to state code
         *
         * @param value stat code
         * @return state
         */
        public static State fromValue(byte value) {
            switch (value) {
                case 0:
                    return INIT;
                case 1:
                    return PROCESSING;
                case 2:
                    return SUCCESSFUL;
                case 3:
                    return FAILED;
                default:
                    throw new IllegalArgumentException("No tiering state for value [" + value + "]");
            }
        }
    }
}
