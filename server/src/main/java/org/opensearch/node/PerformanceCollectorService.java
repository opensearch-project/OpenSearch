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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * This collects node level performance statistics such as cpu, memory, IO of each node and makes it available for
 * coordinator node to aid in throttling, ranking etc
 */
public class PerformanceCollectorService implements ClusterStateListener {
    private final ConcurrentMap<String, PerformanceCollectorService.NodePerformanceStatistics> nodeIdToPerfStats = ConcurrentCollections
        .newConcurrentMap();

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

    public void addNodePerfStatistics(String nodeId, double cpuUsage, double ioUtilization, double memoryUsage, long timestamp) {
        nodeIdToPerfStats.compute(nodeId, (id, ns) -> {
            if (ns == null) {
                return new PerformanceCollectorService.NodePerformanceStatistics(nodeId, cpuUsage, ioUtilization, memoryUsage, timestamp);
            } else {
                ns.cpuPercent = cpuUsage;
                ns.memoryPercent = memoryUsage;
                ns.ioUtilizationPercent = ioUtilization;
                ns.timestamp = timestamp;
                return ns;
            }
        });
    }

    /**
     * Get all node statistics which will be used for node stats
     */
    public Map<String, PerformanceCollectorService.NodePerformanceStatistics> getAllNodeStatistics() {
        Map<String, NodePerformanceStatistics> nodeStats = new HashMap<>(nodeIdToPerfStats.size());
        nodeIdToPerfStats.forEach((k, v) -> { nodeStats.put(k, new PerformanceCollectorService.NodePerformanceStatistics(v)); });
        return nodeStats;
    }

    /**
     * Optionally return a {@code NodePerformanceStatistics} for the given nodeid, if
     * performance stats information exists for the given node. Returns an empty
     * {@code Optional} if the node was not found.
     */
    public Optional<NodePerformanceStatistics> getNodeStatistics(final String nodeId) {
        return Optional.ofNullable(nodeIdToPerfStats.get(nodeId)).map(ns -> new NodePerformanceStatistics(ns));
    }

    public DownstreamNodesPerfStats stats() {
        return new DownstreamNodesPerfStats(getAllNodeStatistics());
    }

    /**
     * This represents the performance stats of a node along with the timestamp at which the stats object was created
     * in the respective node
     */
    public static class NodePerformanceStatistics implements Writeable {
        final String nodeId;
        long timestamp;
        double cpuPercent;
        double ioUtilizationPercent;
        double memoryPercent;

        public NodePerformanceStatistics(
            String nodeId,
            double cpuPercent,
            double ioUtilizationPercent,
            double memoryPercent,
            long timestamp
        ) {
            this.nodeId = nodeId;
            this.cpuPercent = cpuPercent;
            this.ioUtilizationPercent = ioUtilizationPercent;
            this.memoryPercent = memoryPercent;
            this.timestamp = timestamp;
        }

        public NodePerformanceStatistics(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.cpuPercent = in.readDouble();
            this.ioUtilizationPercent = in.readDouble();
            this.memoryPercent = in.readDouble();
            this.timestamp = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.nodeId);
            out.writeDouble(this.cpuPercent);
            out.writeDouble(this.ioUtilizationPercent);
            out.writeDouble(this.memoryPercent);
            out.writeLong(this.timestamp);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("NodePerformanceStatistics[");
            sb.append(nodeId).append("](");
            sb.append("CPU percent: ").append(String.format(Locale.ROOT, "%.1f", cpuPercent));
            sb.append(", IO utilization percent: ").append(String.format(Locale.ROOT, "%.1f", ioUtilizationPercent));
            sb.append(", Memory utilization percent: ").append(String.format(Locale.ROOT, "%.1f", memoryPercent));
            sb.append(", Timestamp: ").append(memoryPercent);
            sb.append(")");
            return sb.toString();
        }

        NodePerformanceStatistics(NodePerformanceStatistics nodeStats) {
            this(nodeStats.nodeId, nodeStats.cpuPercent, nodeStats.ioUtilizationPercent, nodeStats.memoryPercent, nodeStats.timestamp);
        }

        public double getMemoryPercent() {
            return memoryPercent;
        }

        public double getCpuPercent() {
            return cpuPercent;
        }

        public double getIoUtilizationPercent() {
            return ioUtilizationPercent;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

}
