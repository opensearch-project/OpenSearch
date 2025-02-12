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
import org.opensearch.node.resource.tracker.NodeResourceUsageTracker;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * This collects node level resource usage statistics such as cpu, memory, IO of each node and makes it available for
 * coordinator node to aid in throttling, ranking etc
 */
public class ResourceUsageCollectorService extends AbstractLifecycleComponent implements ClusterStateListener {

    /**
     * This refresh interval denotes the polling interval of ResourceUsageCollectorService to refresh the resource usage
     * stats from local node
     */
    private static long REFRESH_INTERVAL_IN_MILLIS = 1000;

    private static final Logger logger = LogManager.getLogger(ResourceUsageCollectorService.class);
    private final ConcurrentMap<String, NodeResourceUsageStats> nodeIdToResourceUsageStats = ConcurrentCollections.newConcurrentMap();

    private ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledFuture;

    private NodeResourceUsageTracker nodeResourceUsageTracker;
    private ClusterService clusterService;

    public ResourceUsageCollectorService(
        NodeResourceUsageTracker nodeResourceUsageTracker,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.threadPool = threadPool;
        this.nodeResourceUsageTracker = nodeResourceUsageTracker;
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                removeNodeResourceUsageStats(removedNode.getId());
            }
        }
    }

    void removeNodeResourceUsageStats(String nodeId) {
        nodeIdToResourceUsageStats.remove(nodeId);
    }

    /**
     * Collect node resource usage stats along with the timestamp
     */
    public void collectNodeResourceUsageStats(
        String nodeId,
        long timestamp,
        double memoryUtilizationPercent,
        double cpuUtilizationPercent,
        IoUsageStats ioUsageStats
    ) {
        nodeIdToResourceUsageStats.compute(nodeId, (id, resourceUsageStats) -> {
            if (resourceUsageStats == null) {
                return new NodeResourceUsageStats(nodeId, timestamp, memoryUtilizationPercent, cpuUtilizationPercent, ioUsageStats);
            } else {
                resourceUsageStats.cpuUtilizationPercent = cpuUtilizationPercent;
                resourceUsageStats.memoryUtilizationPercent = memoryUtilizationPercent;
                resourceUsageStats.setIoUsageStats(ioUsageStats);
                resourceUsageStats.timestamp = timestamp;
                return resourceUsageStats;
            }
        });
    }

    /**
     * Get all node resource usage statistics which will be used for node stats
     */
    public Map<String, NodeResourceUsageStats> getAllNodeStatistics() {
        Map<String, NodeResourceUsageStats> nodeStats = new HashMap<>(nodeIdToResourceUsageStats.size());
        nodeIdToResourceUsageStats.forEach((nodeId, resourceUsageStats) -> {
            nodeStats.put(nodeId, new NodeResourceUsageStats(resourceUsageStats));
        });
        return nodeStats;
    }

    /**
     * Optionally return a {@code NodeResourceUsageStats} for the given nodeid, if
     * resource usage stats information exists for the given node. Returns an empty
     * {@code Optional} if the node was not found.
     */
    public Optional<NodeResourceUsageStats> getNodeStatistics(final String nodeId) {
        return Optional.ofNullable(nodeIdToResourceUsageStats.get(nodeId))
            .map(resourceUsageStats -> new NodeResourceUsageStats(resourceUsageStats));
    }

    /**
     * Returns collected resource usage statistics of all nodes
     */
    public NodesResourceUsageStats stats() {
        return new NodesResourceUsageStats(getAllNodeStatistics());
    }

    /**
     * Fetch local node resource usage statistics and add it to store along with the current timestamp
     */
    private void collectLocalNodeResourceUsageStats() {
        if (nodeResourceUsageTracker.isReady() && clusterService.state() != null) {
            collectNodeResourceUsageStats(
                clusterService.state().nodes().getLocalNodeId(),
                System.currentTimeMillis(),
                nodeResourceUsageTracker.getMemoryUtilizationPercent(),
                nodeResourceUsageTracker.getCpuUtilizationPercent(),
                nodeResourceUsageTracker.getIoUsageStats()
            );
        }
    }

    @Override
    protected void doStart() {
        /**
         * Fetch local node resource usage statistics every second
         */
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                collectLocalNodeResourceUsageStats();
            } catch (Exception e) {
                logger.warn("failure in ResourceUsageCollectorService", e);
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
