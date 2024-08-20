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
import org.opensearch.action.admin.indices.tiering.TieringRequests;
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
import java.util.Map;
import java.util.Set;

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
    static final String TIERING_START_TIME = "start_time";
    private TieringRequests tieringRequests;
    private final PendingStartTieringStateProcessor pendingStartTieringStateProcessor;
    private final RunningTieringStateProcessor runningTieringStateProcessor;

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
        // TODO: initialize the queue size via the cluster based setting
        this.pendingStartTieringStateProcessor = new PendingStartTieringStateProcessor(allocationService);
        this.runningTieringStateProcessor = new RunningTieringStateProcessor(allocationService);
        if (DiscoveryNode.isClusterManagerNode(settings) && FeatureFlags.isEnabled(FeatureFlags.TIERED_REMOTE_INDEX)) {
            clusterService.addListener(this);
            this.tieringRequests = new TieringRequests(100);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO: https://github.com/opensearch-project/OpenSearch/issues/14981
        if (event.localNodeClusterManager()) {
            if (!tieringRequests.getQueuedTieringContexts().isEmpty()) {
                pendingStartTieringStateProcessor.process(event.state(), clusterService, tieringRequests);
            }
            if (event.routingTableChanged()) {
                if (!tieringRequests.getAcceptedTieringRequestContexts().isEmpty()) {
                    runningTieringStateProcessor.process(event.state(), clusterService, tieringRequests);
                }
            }
        }
    }

    /**
     * Updates the index metadata with the tiering settings/metadata for an accepted index.
     * Accepted index is an index to be tiered from hot to warm.
     * @param metadataBuilder metadata builder
     * @param indexMetadata index metadata
     */
    void updateIndexMetadataForAcceptedIndex(
        final Metadata.Builder metadataBuilder,
        final IndexMetadata indexMetadata
    ) {
        Settings.Builder indexSettingsBuilder = Settings.builder().put(indexMetadata.getSettings());
        // update index settings here
        indexSettingsBuilder.put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT_TO_WARM);

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
                for (Index index : tieringRequestContext.getIndicesPendingTiering()) {
                    final IndexMetadata indexMetadata = currentState.metadata().index(index);
                    if (indexMetadata == null) {
                        tieringRequestContext.markIndexFailed(index, "index not found");
                        continue;
                    } else if (!indexMetadata.isHotIndex()) {
                        tieringRequestContext.markIndexFailed(index, "index is not in the HOT tier");
                        continue;
                    }
                    updateIndexMetadataForAcceptedIndex(metadataBuilder, indexMetadata);
                }
                return ClusterState.builder(currentState)
                    .metadata(metadataBuilder)
                    .routingTable(routingTableBuilder.build())
                    .build();
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
                tieringRequests.addToPending(tieringRequestContext);
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
