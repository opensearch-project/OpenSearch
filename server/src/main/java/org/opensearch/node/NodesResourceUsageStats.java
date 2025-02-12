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
 * This class represents resource usage stats such as CPU, Memory and IO resource usage of each node along with the
 * timestamp of the stats recorded.
 */
public class NodesResourceUsageStats implements Writeable, ToXContentFragment {

    // Map of node id to resource usage stats of the corresponding node.
    private final Map<String, NodeResourceUsageStats> nodeIdToResourceUsageStatsMap;

    public NodesResourceUsageStats(Map<String, NodeResourceUsageStats> nodeIdToResourceUsageStatsMap) {
        this.nodeIdToResourceUsageStatsMap = nodeIdToResourceUsageStatsMap;
    }

    public NodesResourceUsageStats(StreamInput in) throws IOException {
        this.nodeIdToResourceUsageStatsMap = in.readMap(StreamInput::readString, NodeResourceUsageStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.nodeIdToResourceUsageStatsMap, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
    }

    /**
     * Returns map of node id to resource usage stats of the corresponding node.
     */
    public Map<String, NodeResourceUsageStats> getNodeIdToResourceUsageStatsMap() {
        return nodeIdToResourceUsageStatsMap;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("resource_usage_stats");
        for (String nodeId : nodeIdToResourceUsageStatsMap.keySet()) {
            builder.startObject(nodeId);
            NodeResourceUsageStats resourceUsageStats = nodeIdToResourceUsageStatsMap.get(nodeId);
            if (resourceUsageStats != null) {
                builder.field("timestamp", resourceUsageStats.timestamp);
                builder.field("cpu_utilization_percent", String.format(Locale.ROOT, "%.1f", resourceUsageStats.cpuUtilizationPercent));
                builder.field(
                    "memory_utilization_percent",
                    String.format(Locale.ROOT, "%.1f", resourceUsageStats.memoryUtilizationPercent)
                );
                if (resourceUsageStats.getIoUsageStats() != null) {
                    builder.field("io_usage_stats", resourceUsageStats.getIoUsageStats());
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
