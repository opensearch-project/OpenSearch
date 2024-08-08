/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.opensearch.action.admin.indices.tiering.HotToWarmTieringResponse;
import org.opensearch.action.admin.indices.tiering.TieringRequestContext;
import org.opensearch.action.admin.indices.tiering.TieringUpdateClusterStateRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.action.admin.indices.tiering.TieringUtils.constructToHotToWarmTieringResponse;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.TIERING_CUSTOM_KEY;
import static org.opensearch.index.IndexModule.INDEX_STORE_LOCALITY_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;

/**
 * Service responsible for tiering indices from hot to warm
 * @opensearch.experimental
 */
@ExperimentalApi
public class HotToWarmTieringService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(HotToWarmTieringService.class);
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AllocationService allocationService;
    private final Set<TieringRequestContext> tieringRequestContexts = ConcurrentHashMap.newKeySet();
    static final String TIERING_START_TIME = "start_time";

    @Inject
    public HotToWarmTieringService(
        Settings settings,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.allocationService = allocationService;

        if (DiscoveryNode.isClusterManagerNode(settings) && FeatureFlags.isEnabled(FeatureFlags.TIERED_REMOTE_INDEX)) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO: https://github.com/opensearch-project/OpenSearch/issues/14981
        if (event.routingTableChanged()) {
            if (!tieringRequestContexts.isEmpty()) {
                processTieringRequestContexts(event.state());
            }
        }
    }

    void processTieringRequestContexts(final ClusterState clusterState) {
        final Map<Index, TieringRequestContext> tieredIndices = new HashMap<>();
        for (TieringRequestContext tieringRequestContext : tieringRequestContexts) {
            if (tieringRequestContext.isRequestProcessingComplete()) {
                logger.info("[HotToWarmTiering] Tiering is completed for the request [{}]", tieringRequestContext);
                completeRequestLevelTiering(tieringRequestContext);
                continue;
            }
            List<ShardRouting> shardRoutings;
            for (Index index : tieringRequestContext.getInProgressIndices()) {
                if (clusterState.routingTable().hasIndex(index)) {
                    // Ensure index is not deleted
                    shardRoutings = clusterState.routingTable().allShards(index.getName());
                } else {
                    // Index already deleted nothing to do
                    logger.warn("[HotToWarmTiering] Index [{}] deleted before shard relocation finished", index.getName());
                    tieringRequestContext.addToFailed(index, "index not found");
                    continue;
                }

                boolean relocationCompleted = true;
                for (ShardRouting shard : shardRoutings) {
                    if (!isShardInWarmTier(shard, clusterState)) {
                        relocationCompleted = false;
                        break;
                    }
                }
                if (relocationCompleted) {
                    logger.debug("[HotToWarmTiering] Shard relocation completed for index [{}]", index.getName());
                    tieringRequestContext.addToTiered(index);
                    tieredIndices.put(index, tieringRequestContext);
                }
            }
        }
        if (!tieredIndices.isEmpty()) {
            updateClusterStateForTieredIndices(tieredIndices);
        }
    }

    /**
     * Checks if the shard is in the warm tier.
     * @param shard shard routing
     * @param clusterState current cluster state
     * @return true if shard is started on the search node, false otherwise
     */
    boolean isShardInWarmTier(final ShardRouting shard, final ClusterState clusterState) {
        if (shard.unassigned()) {
            return false;
        }
        final boolean isShardFoundOnSearchNode = clusterState.getNodes().get(shard.currentNodeId()).isSearchNode();
        return shard.started() && isShardFoundOnSearchNode;
    }

    /**
     * Completes the request level tiering for requestContext.
     * @param requestContext tiering request context
     */
    void completeRequestLevelTiering(TieringRequestContext requestContext) {
        tieringRequestContexts.remove(requestContext);
        if (requestContext.getListener() != null) {
            requestContext.getListener().onResponse(constructToHotToWarmTieringResponse(requestContext.getFailedIndices()));
        }
    }

    /**
     * Updates the request context for tiered indices,
     * Moves tiered indices to successful state,
     * Checks and completes the request level tiering
     * @param tieredIndices map of tiered indices and their request contexts
     */
    void updateRequestContextForTieredIndices(final Map<Index, TieringRequestContext> tieredIndices) {
        for (Map.Entry<Index, TieringRequestContext> entry : tieredIndices.entrySet()) {
            Index index = entry.getKey();
            TieringRequestContext tieringRequestContext = entry.getValue();
            tieringRequestContext.addToCompleted(index);
            if (tieringRequestContext.isRequestProcessingComplete()) {
                logger.info("[HotToWarmTiering] Tiering is completed for the request [{}]", tieringRequestContext);
                completeRequestLevelTiering(tieringRequestContext);
            }
        }
    }

    /**
     * Updates the index metadata with the tiering settings/metadata for an accepted index.
     * Accepted index is an index to be tiered from hot to warm.
     * @param metadataBuilder metadata builder
     * @param routingTableBuilder routing builder
     * @param indexMetadata index metadata
     * @param index index
     */
    void updateIndexMetadataForAcceptedIndex(
        final Metadata.Builder metadataBuilder,
        final RoutingTable.Builder routingTableBuilder,
        final IndexMetadata indexMetadata,
        final Index index
    ) {
        Settings.Builder indexSettingsBuilder = Settings.builder().put(indexMetadata.getSettings());
        // update index settings here
        indexSettingsBuilder.put(INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL);
        indexSettingsBuilder.put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT_TO_WARM);

        // Update number of replicas to 1 in case the number of replicas is greater than 1
        if (Integer.parseInt(indexMetadata.getSettings().get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey())) > 1) {
            final String[] indices = new String[] { index.getName() };
            routingTableBuilder.updateNumberOfReplicas(1, indices);
            metadataBuilder.updateNumberOfReplicas(1, indices);
        }
        // trying to put transient index metadata in the custom index metadata
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata).settings(indexSettingsBuilder);
        final Map<String, String> tieringCustomData = new HashMap<>();
        tieringCustomData.put(TIERING_START_TIME, String.valueOf(System.currentTimeMillis()));
        indexMetadataBuilder.putCustom(TIERING_CUSTOM_KEY, tieringCustomData);
        // Update index settings version
        indexMetadataBuilder.settingsVersion(1 + indexMetadataBuilder.settingsVersion());
        metadataBuilder.put(indexMetadataBuilder);
    }

    /**
     * Updates the cluster state by updating the index metadata for tiered indices.
     * @param tieredIndices set of tiered indices
     */
    void updateClusterStateForTieredIndices(final Map<Index, TieringRequestContext> tieredIndices) {
        clusterService.submitStateUpdateTask(
            "complete hot to warm tiering for tiered indices: " + tieredIndices.keySet(),
            new ClusterStateUpdateTask(Priority.NORMAL) {

                @Override
                public ClusterState execute(ClusterState currentState) {
                    final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    for (Map.Entry<Index, TieringRequestContext> entry : tieredIndices.entrySet()) {
                        Index index = entry.getKey();
                        final IndexMetadata indexMetadata = currentState.metadata().index(index);
                        if (indexMetadata == null) {
                            entry.getValue().addToFailed(index, "index not found");
                            continue;
                        }
                        updateIndexMetadataForTieredIndex(metadataBuilder, indexMetadata);
                    }
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "[HotToWarmTiering] failed to complete tiering for tiered indices " + "[{}]",
                            tieredIndices
                        ),
                        e
                    );
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("[HotToWarmTiering] Cluster state updated for source " + source);
                    updateRequestContextForTieredIndices(tieredIndices);
                }
            }
        );
    }

    /**
     * Updates the index metadata with the tiering settings/metadata for a tiered index.
     * @param metadataBuilder metadata builder
     * @param indexMetadata index metadata
     */
    void updateIndexMetadataForTieredIndex(final Metadata.Builder metadataBuilder, final IndexMetadata indexMetadata) {
        Settings.Builder indexSettingsBuilder = Settings.builder().put(indexMetadata.getSettings());
        // update tiering settings here
        indexSettingsBuilder.put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM);
        // trying to put transient index metadata in the custom index metadata
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata).settings(indexSettingsBuilder);
        indexMetadataBuilder.removeCustom(TIERING_CUSTOM_KEY);

        // Update index settings version
        indexMetadataBuilder.settingsVersion(1 + indexMetadataBuilder.settingsVersion());
        metadataBuilder.put(indexMetadataBuilder);
    }

    /**
     * Tier indices from hot to warm
     * @param request - tiering update cluster state request
     * @param listener - call back listener
     */
    public void tier(final TieringUpdateClusterStateRequest request, final ActionListener<HotToWarmTieringResponse> listener) {
        final Set<Index> indices = Set.of(request.indices());
        final TieringRequestContext tieringRequestContext = new TieringRequestContext(
            request.waitForCompletion() ? listener : null,
            indices,
            request.getRejectedIndices()
        );

        logger.info("[HotToWarmTiering] Starting hot to warm tiering for indices {}", indices);
        clusterService.submitStateUpdateTask("start hot to warm tiering: " + indices, new ClusterStateUpdateTask(Priority.URGENT) {

            @Override
            public ClusterState execute(ClusterState currentState) {
                final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                for (Index index : tieringRequestContext.getInProgressIndices()) {
                    final IndexMetadata indexMetadata = currentState.metadata().index(index);
                    if (indexMetadata == null) {
                        tieringRequestContext.addToFailed(index, "index not found");
                        continue;
                    } else if (indexMetadata.isHotIndex()) {
                        tieringRequestContext.addToFailed(index, "index is not in the HOT tier");
                        continue;
                    }
                    updateIndexMetadataForAcceptedIndex(metadataBuilder, routingTableBuilder, indexMetadata, index);
                }
                ClusterState updatedState = ClusterState.builder(currentState)
                    .metadata(metadataBuilder)
                    .routingTable(routingTableBuilder.build())
                    .build();

                // now, reroute to trigger shard relocation for the dedicated case
                updatedState = allocationService.reroute(updatedState, "hot to warm tiering");

                return updatedState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage("[HotToWarmTiering] failed tiering for indices " + "[{}]", indices),
                    e
                );
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("[HotToWarmTiering] Cluster state updated for source " + source);
                tieringRequestContexts.add(tieringRequestContext);
                if (!request.waitForCompletion()) {
                    listener.onResponse(constructToHotToWarmTieringResponse(tieringRequestContext.getFailedIndices()));
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

}
