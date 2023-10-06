/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.ratelimiting.tracker.NodePerformanceTracker;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * This collects node level performance statistics such as cpu, memory, IO of each node and makes it available for
 * coordinator node to aid in throttling, ranking etc
 */
public class PerformanceCollectorService extends AbstractLifecycleComponent implements ClusterStateListener {

    /**
     * This refresh interval denotes the polling interval of PerformanceCollectorService to refresh the performance stats
     * from local node
     */
    private static long REFRESH_INTERVAL_IN_MILLIS = 1000;

    private static final Logger logger = LogManager.getLogger(PerformanceCollectorService.class);
    private final ConcurrentMap<String, NodePerformanceStats> nodeIdToPerfStats = ConcurrentCollections.newConcurrentMap();

    private ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledFuture;

    private NodePerformanceTracker nodePerformanceTracker;
    private ClusterService clusterService;

    public PerformanceCollectorService(
        NodePerformanceTracker nodePerformanceTracker,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.threadPool = threadPool;
        this.nodePerformanceTracker = nodePerformanceTracker;
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                removeNodePerfStatistics(removedNode.getId());
            }
        }
    }

    void removeNodePerfStatistics(String nodeId) {
        nodeIdToPerfStats.remove(nodeId);
    }

    /**
     * Collect node performance statistics along with the timestamp
     */
    public void collectNodePerfStatistics(String nodeId, long timestamp, double memoryUtilizationPercent, double cpuUtilizationPercent) {
        nodeIdToPerfStats.compute(nodeId, (id, nodePerfStats) -> {
            if (nodePerfStats == null) {
                return new NodePerformanceStats(nodeId, timestamp, memoryUtilizationPercent, cpuUtilizationPercent);
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
    public Map<String, NodePerformanceStats> getAllNodeStatistics() {
        Map<String, NodePerformanceStats> nodeStats = new HashMap<>(nodeIdToPerfStats.size());
        nodeIdToPerfStats.forEach((nodeId, nodePerfStats) -> { nodeStats.put(nodeId, new NodePerformanceStats(nodePerfStats)); });
        return nodeStats;
    }

    /**
     * Optionally return a {@code NodePerformanceStatistics} for the given nodeid, if
     * performance stats information exists for the given node. Returns an empty
     * {@code Optional} if the node was not found.
     */
    public Optional<NodePerformanceStats> getNodeStatistics(final String nodeId) {
        return Optional.ofNullable(nodeIdToPerfStats.get(nodeId)).map(perfStats -> new NodePerformanceStats(perfStats));
    }

    /**
     * Returns collected performance statistics of all nodes
     */
    public NodesPerformanceStats stats() {
        return new NodesPerformanceStats(getAllNodeStatistics());
    }

    /**
     * Fetch local node performance statistics and add it to store along with the current timestamp
     */
    private void collectLocalNodePerformanceStats() {
        if (nodePerformanceTracker.isReady() && clusterService.state() != null) {
            collectNodePerfStatistics(
                clusterService.state().nodes().getLocalNodeId(),
                System.currentTimeMillis(),
                nodePerformanceTracker.getMemoryUtilizationPercent(),
                nodePerformanceTracker.getCpuUtilizationPercent()
            );
        }
    }

    @Override
    protected void doStart() {
        /**
         * Fetch local node performance statistics every second
         */
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                collectLocalNodePerformanceStats();
            } catch (Exception e) {
                logger.warn("failure in PerformanceCollectorService", e);
            }
        }, new TimeValue(REFRESH_INTERVAL_IN_MILLIS), ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() {}
}
