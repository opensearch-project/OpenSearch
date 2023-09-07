/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This class represents collected performance stats of all downstream nodes and the local node
 */
public class DownstreamNodesPerfStats implements Writeable, ToXContentFragment {
    private final Map<String, PerformanceCollectorService.NodePerformanceStatistics> nodePerfStats;

    public DownstreamNodesPerfStats(Map<String, PerformanceCollectorService.NodePerformanceStatistics> nodePerfStats) {
        this.nodePerfStats = nodePerfStats;
    }

    public DownstreamNodesPerfStats(StreamInput in) throws IOException {
        this.nodePerfStats = in.readMap(StreamInput::readString, PerformanceCollectorService.NodePerformanceStatistics::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.nodePerfStats, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
    }

    /**
     * Returns map of node id to perf stats
     */
    public Map<String, PerformanceCollectorService.NodePerformanceStatistics> getNodePerfStats() {
        return nodePerfStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes_performance_stats");
        for (String nodeId : nodePerfStats.keySet()) {
            builder.startObject(nodeId);
            PerformanceCollectorService.NodePerformanceStatistics perfStats = nodePerfStats.get(nodeId);
            if (perfStats != null) {

                builder.field("cpu_usage_percent", String.format(Locale.ROOT, "%.1f", perfStats.cpuPercent));
                builder.field("memory_usage_percent", String.format(Locale.ROOT, "%.1f", perfStats.memoryPercent));
                builder.field("io_usage_percent", String.format(Locale.ROOT, "%.1f", perfStats.ioUtilizationPercent));
                builder.field(
                    "elapsed_time",
                    new TimeValue(System.currentTimeMillis() - perfStats.timestamp, TimeUnit.MILLISECONDS).toString()
                );
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
