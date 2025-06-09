/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

import static org.opensearch.node.NodeResourceUsageStats.Fields.CPU_UTILIZATION_PERCENT;
import static org.opensearch.node.NodeResourceUsageStats.Fields.IO_USAGE_STATS;
import static org.opensearch.node.NodeResourceUsageStats.Fields.MEMORY_UTILIZATION_PERCENT;
import static org.opensearch.node.NodeResourceUsageStats.Fields.TIMESTAMP;

/**
 * This represents the resource usage stats of a node along with the timestamp at which the stats object was created
 * in the respective node
 */
@ExperimentalApi
public class NodeResourceUsageStats implements Writeable, ToXContentFragment {
    final String nodeId;
    long timestamp;
    double cpuUtilizationPercent;
    double memoryUtilizationPercent;
    private IoUsageStats ioUsageStats;

    public NodeResourceUsageStats(
        String nodeId,
        long timestamp,
        double memoryUtilizationPercent,
        double cpuUtilizationPercent,
        IoUsageStats ioUsageStats
    ) {
        this.nodeId = nodeId;
        this.timestamp = timestamp;
        this.cpuUtilizationPercent = cpuUtilizationPercent;
        this.memoryUtilizationPercent = memoryUtilizationPercent;
        this.ioUsageStats = ioUsageStats;
    }

    public NodeResourceUsageStats(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.timestamp = in.readLong();
        this.cpuUtilizationPercent = in.readDouble();
        this.memoryUtilizationPercent = in.readDouble();
        if (in.getVersion().onOrAfter(Version.V_2_13_0)) {
            this.ioUsageStats = in.readOptionalWriteable(IoUsageStats::new);
        } else {
            this.ioUsageStats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeLong(this.timestamp);
        out.writeDouble(this.cpuUtilizationPercent);
        out.writeDouble(this.memoryUtilizationPercent);
        if (out.getVersion().onOrAfter(Version.V_2_13_0)) {
            out.writeOptionalWriteable(this.ioUsageStats);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("NodeResourceUsageStats[");
        sb.append(nodeId).append("](");
        sb.append("Timestamp: ").append(timestamp);
        sb.append(", CPU utilization percent: ").append(String.format(Locale.ROOT, "%.1f", this.getCpuUtilizationPercent()));
        sb.append(", Memory utilization percent: ").append(String.format(Locale.ROOT, "%.1f", this.getMemoryUtilizationPercent()));
        if (this.ioUsageStats != null) {
            sb.append(", ").append(this.getIoUsageStats());
        }
        sb.append(")");
        return sb.toString();
    }

    NodeResourceUsageStats(NodeResourceUsageStats nodeResourceUsageStats) {
        this(
            nodeResourceUsageStats.nodeId,
            nodeResourceUsageStats.timestamp,
            nodeResourceUsageStats.memoryUtilizationPercent,
            nodeResourceUsageStats.cpuUtilizationPercent,
            nodeResourceUsageStats.ioUsageStats
        );
    }

    public double getMemoryUtilizationPercent() {
        return memoryUtilizationPercent;
    }

    public double getCpuUtilizationPercent() {
        return cpuUtilizationPercent;
    }

    public IoUsageStats getIoUsageStats() {
        return ioUsageStats;
    }

    public void setIoUsageStats(IoUsageStats ioUsageStats) {
        this.ioUsageStats = ioUsageStats;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(nodeId);
        builder.field(TIMESTAMP, timestamp);
        builder.field(CPU_UTILIZATION_PERCENT, String.format(Locale.ROOT, "%.1f", cpuUtilizationPercent));
        builder.field(MEMORY_UTILIZATION_PERCENT, String.format(Locale.ROOT, "%.1f", memoryUtilizationPercent));
        if (ioUsageStats != null) {
            builder.field(IO_USAGE_STATS, ioUsageStats);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Fields used for statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String TIMESTAMP = "timestamp";
        static final String CPU_UTILIZATION_PERCENT = "cpu_utilization_percent";
        static final String MEMORY_UTILIZATION_PERCENT = "memory_utilization_percent";
        static final String IO_USAGE_STATS = "io_usage_stats";
    }
}
