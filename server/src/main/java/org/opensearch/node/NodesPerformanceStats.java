/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * This class represents performance stats such as CPU, Memory and IO resource usage of each node along with the time
 * elapsed from when the stats were recorded.
 */
public class NodesPerformanceStats implements Writeable, ToXContentFragment {

    // Map of node id to perf stats of the corresponding node.
    private final Map<String, NodePerformanceStats> nodeIdToPerfStatsMap;

    public NodesPerformanceStats(Map<String, NodePerformanceStats> nodeIdToPerfStatsMap) {
        this.nodeIdToPerfStatsMap = nodeIdToPerfStatsMap;
    }

    public NodesPerformanceStats(StreamInput in) throws IOException {
        this.nodeIdToPerfStatsMap = in.readMap(StreamInput::readString, NodePerformanceStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.nodeIdToPerfStatsMap, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
    }

    /**
     * Returns map of node id to perf stats of the corresponding node.
     */
    public Map<String, NodePerformanceStats> getNodeIdToNodePerfStatsMap() {
        return nodeIdToPerfStatsMap;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("performance_stats");
        for (String nodeId : nodeIdToPerfStatsMap.keySet()) {
            builder.startObject(nodeId);
            NodePerformanceStats perfStats = nodeIdToPerfStatsMap.get(nodeId);
            if (perfStats != null) {
                builder.field("timestamp", perfStats.timestamp);
                builder.field("cpu_utilization_percent", String.format(Locale.ROOT, "%.1f", perfStats.cpuUtilizationPercent));
                builder.field("memory_utilization_percent", String.format(Locale.ROOT, "%.1f", perfStats.memoryUtilizationPercent));
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
