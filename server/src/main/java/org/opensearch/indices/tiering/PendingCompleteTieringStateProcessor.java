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
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;

import java.util.Map;

import static org.opensearch.cluster.metadata.IndexMetadata.TIERING_CUSTOM_KEY;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;

public class PendingCompleteTieringStateProcessor extends AbstractTieringStateProcessor {

    private static final Logger logger = LogManager.getLogger(PendingCompleteTieringStateProcessor.class);

    public PendingCompleteTieringStateProcessor(AllocationService allocationService) {
        super(IndexTieringState.COMPLETED, allocationService);
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

    @Override
    public void process(ClusterState clusterState, ClusterService clusterService, TieringRequests tieringRequests) {
        clusterService.submitStateUpdateTask(
            "complete hot to warm tiering for tiered indices: ",
            new ClusterStateUpdateTask(Priority.NORMAL) {
                final Map<Index, TieringRequestContext> indices = tieringRequests.getIndices(tieringRequests.getAcceptedTieringRequestContexts(), IndexTieringState.PENDING_COMPLETION);

                @Override
                public ClusterState execute(ClusterState currentState) {
                    final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    for (Map.Entry<Index, TieringRequestContext> entry : indices.entrySet()) {
                        Index index = entry.getKey();
                        final IndexMetadata indexMetadata = currentState.metadata().index(index);
                        if (indexMetadata == null) {
                            entry.getValue().markIndexFailed(index, "index not found");
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
                            indices
                        ),
                        e
                    );
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("[HotToWarmTiering] Cluster state updated for source " + source);
                    for (Map.Entry<Index, TieringRequestContext> entry : indices.entrySet()) {
                        if (!entry.getValue().hasIndexFailed(entry.getKey())) {
                            entry.getValue().markIndexAsCompleted(entry.getKey());
                        }
                    }
                    assert nextStateProcessor != null;
                    nextStateProcessor.process(clusterState, clusterService, tieringRequests);
                }
            }
        );
    }
}
