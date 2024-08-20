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
import org.opensearch.action.admin.indices.tiering.TieringRequests;
import org.opensearch.action.admin.indices.tiering.TieringRequestContext;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.index.IndexModule.INDEX_STORE_LOCALITY_SETTING;

public class PendingStartTieringStateProcessor extends AbstractTieringStateProcessor {

    private static final Logger logger = LogManager.getLogger(PendingStartTieringStateProcessor.class);

    public PendingStartTieringStateProcessor(AllocationService allocationService) {
        super(IndexTieringState.IN_PROGRESS, allocationService);
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

        // Update number of replicas to 1 in case the number of replicas is greater than 1
        if (Integer.parseInt(indexMetadata.getSettings().get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey())) > 1) {
            final String[] indices = new String[] { index.getName() };
            routingTableBuilder.updateNumberOfReplicas(1, indices);
            metadataBuilder.updateNumberOfReplicas(1, indices);
        }
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata).settings(indexSettingsBuilder);
        // Update index settings version
        indexMetadataBuilder.settingsVersion(1 + indexMetadataBuilder.settingsVersion());
        metadataBuilder.put(indexMetadataBuilder);
    }


    @Override
    public void process(ClusterState clusterState, ClusterService clusterService, TieringRequests tieringRequests) {

        clusterService.submitStateUpdateTask("start hot to warm tiering: ", new ClusterStateUpdateTask(Priority.URGENT) {
            final Set<TieringRequestContext> tieringRequestContexts = tieringRequests.dequePendingTieringContexts();
            final Map<Index, TieringRequestContext> indices = tieringRequests.getIndices(tieringRequestContexts, IndexTieringState.PENDING_START);

            @Override
            public ClusterState execute(ClusterState currentState) {
                final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                for (Map.Entry<Index, TieringRequestContext> entry : indices.entrySet()) {
                    Index index = entry.getKey();
                    final IndexMetadata indexMetadata = currentState.metadata().index(index);
                    if (indexMetadata == null) {
                        indices.get(index).markIndexFailed(index, "index not found");
                        continue;
                    } else if (!indexMetadata.isIndexInTier(IndexModule.TieringState.HOT_TO_WARM)) {
                        indices.get(index).markIndexFailed(index, "index is not in the HOT tier");
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
                // process failed request here and add the retry logic
                logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage("[HotToWarmTiering] failed tiering for indices " + "[{}]", indices),
                    e
                );
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("[HotToWarmTiering] Cluster state updated for source " + source);
                for (Map.Entry<Index, TieringRequestContext> entry : indices.entrySet()) {
                    if (!entry.getValue().hasIndexFailed(entry.getKey())) {
                        entry.getValue().markIndexInProgress(entry.getKey());
                    }
                }
                tieringRequests.addToAccepted(tieringRequestContexts);
                assert nextStateProcessor != null;
                nextStateProcessor.process(clusterState, clusterService, tieringRequests);
            }
        });
    }
}
