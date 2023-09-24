/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ConcurrentCollections;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * This collects node level performance statistics such as cpu, memory, IO of each node and makes it available for
 * coordinator node to aid in throttling, ranking etc
 */
public class PerformanceCollectorService implements ClusterStateListener {
    private final ConcurrentMap<String, NodePerformanceStatistics> nodeIdToPerfStats = ConcurrentCollections.newConcurrentMap();

    public PerformanceCollectorService(ClusterService clusterService) {
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                removeNode(removedNode.getId());
            }
        }
    }

    void removeNode(String nodeId) {
        nodeIdToPerfStats.remove(nodeId);
    }

    public void addNodePerfStatistics(String nodeId, double cpuUtilizationPercent, double memoryUtilizationPercent, long timestamp) {
        nodeIdToPerfStats.compute(nodeId, (id, nodePerfStats) -> {
            if (nodePerfStats == null) {
                return new NodePerformanceStatistics(nodeId, cpuUtilizationPercent, memoryUtilizationPercent, timestamp);
            } else {
                nodePerfStats.cpuUtilizationPercent = cpuUtilizationPercent;
                nodePerfStats.memoryUtilizationPercent = memoryUtilizationPercent;
                nodePerfStats.timestamp = timestamp;
                return nodePerfStats;
            }
        });
    }

    /**
     * Get all node statistics which will be used for node stats
     */
    public Map<String, NodePerformanceStatistics> getAllNodeStatistics() {
        Map<String, NodePerformanceStatistics> nodeStats = new HashMap<>(nodeIdToPerfStats.size());
        nodeIdToPerfStats.forEach((nodeId, nodePerfStats) -> { nodeStats.put(nodeId, new NodePerformanceStatistics(nodePerfStats)); });
        return nodeStats;
    }

    /**
     * Optionally return a {@code NodePerformanceStatistics} for the given nodeid, if
     * performance stats information exists for the given node. Returns an empty
     * {@code Optional} if the node was not found.
     */
    public Optional<NodePerformanceStatistics> getNodeStatistics(final String nodeId) {
        return Optional.ofNullable(nodeIdToPerfStats.get(nodeId)).map(perfStats -> new NodePerformanceStatistics(perfStats));
    }

    public GlobalPerformanceStats stats() {
        return new GlobalPerformanceStats(getAllNodeStatistics());
    }

}
