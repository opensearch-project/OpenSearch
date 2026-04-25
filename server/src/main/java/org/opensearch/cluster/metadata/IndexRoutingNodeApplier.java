/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.cluster.ClusterStateUpdateTask;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Applier that creates routing nodes for indices that were created via IMC but don't have routing nodes yet.
 * This applier only runs on the cluster manager node.
 *
 * @opensearch.internal
 */
public class IndexRoutingNodeApplier implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(IndexRoutingNodeApplier.class);

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private volatile boolean enabled = true;

    public IndexRoutingNodeApplier(ClusterService clusterService, AllocationService allocationService) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    /**
     * Disable this applier
     */
    public void disable() {
        this.enabled = false;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (!enabled) {
            return;
        }

        ClusterState newState = event.state();

        // Find indices that exist in metadata but not in routing table (for creation)
        Set<String> indicesWithoutRouting = newState.metadata().indices().keySet().stream()
            .filter(indexName -> !newState.routingTable().hasIndex(indexName))
            .collect(Collectors.toSet());

        // Find indices that exist in routing table but not in metadata (for deletion)
        Set<String> routingWithoutMetadata = newState.routingTable().indicesRouting().keySet().stream()
            .filter(indexName -> !newState.metadata().hasIndex(indexName))
            .collect(Collectors.toSet());

        if (indicesWithoutRouting.isEmpty() && routingWithoutMetadata.isEmpty()) {
            return;
        }

        if (!newState.nodes().isLocalNodeElectedClusterManager()) {
            logger.info("Skipping index routing synchronization as local node is not cluster manager");
            return;
        }

        if (!indicesWithoutRouting.isEmpty()) {
            logger.info("Found {} indices without routing nodes: {}", indicesWithoutRouting.size(), indicesWithoutRouting);
        }
        if (!routingWithoutMetadata.isEmpty()) {
            logger.info("Found {} routing nodes without metadata: {}", routingWithoutMetadata.size(), routingWithoutMetadata);
        }

        // Submit task to synchronize routing table with metadata
        clusterService.submitStateUpdateTask(
            "sync-routing-table-with-metadata",
            new ClusterStateUpdateTask(Priority.HIGH) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                    boolean hasChanges = false;

                    // Add routing for indices that exist in metadata but not in routing table
                    for (String indexName : indicesWithoutRouting) {
                        if (currentState.metadata().hasIndex(indexName) && !currentState.routingTable().hasIndex(indexName)) {
                            IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                            routingTableBuilder.addAsNew(indexMetadata);
                            hasChanges = true;
                            logger.info("Added routing table for index: {}", indexName);
                        }
                    }

                    // Remove routing for indices that exist in routing table but not in metadata
                    for (String indexName : routingWithoutMetadata) {
                        if (!currentState.metadata().hasIndex(indexName) && currentState.routingTable().hasIndex(indexName)) {
                            routingTableBuilder.remove(indexName);
                            hasChanges = true;
                            logger.info("Removed routing table for deleted index: {}", indexName);
                        }
                    }

                    if (!hasChanges) {
                        return currentState;
                    }

                    ClusterState updatedState = ClusterState.builder(currentState)
                        .routingTable(routingTableBuilder.build())
                        .build();

                    // Apply allocation to update shard assignments
                    return allocationService.reroute(updatedState, "sync routing table with metadata");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("Failed to sync routing table with metadata. Indices without routing: {}, Routing without metadata: {}", 
                        indicesWithoutRouting, routingWithoutMetadata, e);
                }
            }
        );
    }
}
