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

        // Find indices that exist in metadata but not in routing table
        Set<String> indicesWithoutRouting = newState.metadata().indices().keySet().stream()
            .filter(indexName -> !newState.routingTable().hasIndex(indexName))
            .collect(Collectors.toSet());

        if (indicesWithoutRouting.isEmpty()) {
            return;
        }

        if (!newState.nodes().isLocalNodeElectedClusterManager()) {
            logger.info("Skipping index routing node creation as local node is not cluster manager");
            return;
        }

        logger.info("Found {} indices without routing nodes: {}", indicesWithoutRouting.size(), indicesWithoutRouting);

        // Submit task to create routing nodes for these indices
        clusterService.submitStateUpdateTask(
            "create-routing-nodes-for-imc-indices",
            new ClusterStateUpdateTask(Priority.HIGH) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                    boolean hasChanges = false;

                    for (String indexName : indicesWithoutRouting) {
                        // Double-check the index still exists and doesn't have routing
                        if (currentState.metadata().hasIndex(indexName) && !currentState.routingTable().hasIndex(indexName)) {
                            IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                            routingTableBuilder.addAsNew(indexMetadata);
                            hasChanges = true;
                            logger.info("Added routing table for index: {}", indexName);
                        }
                    }

                    if (!hasChanges) {
                        return currentState;
                    }

                    ClusterState updatedState = ClusterState.builder(currentState)
                        .routingTable(routingTableBuilder.build())
                        .build();

                    // Apply allocation to create actual shard assignments
                    return allocationService.reroute(updatedState, "create routing nodes for IMC indices");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("Failed to create routing nodes for IMC indices: {}", indicesWithoutRouting, e);
                }
            }
        );
    }
}
