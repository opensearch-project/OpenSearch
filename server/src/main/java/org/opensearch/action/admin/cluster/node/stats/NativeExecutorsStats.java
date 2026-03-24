/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats.RuntimeValues;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats.TaskMonitorValues;

import java.io.IOException;
import java.util.Objects;

/**
 * Server-side Writeable + ToXContentFragment wrapper around the
 * {@link DataFusionPluginStats} POJO.  Handles transport serialization
 * and JSON rendering for the {@code native_executors} section of the
 * nodes-stats API response.
 *
 * @opensearch.internal
 */
public class NativeExecutorsStats implements Writeable, ToXContentFragment {

    private final DataFusionPluginStats dataFusionPluginStats;

    public NativeExecutorsStats(DataFusionPluginStats stats) {
        this.dataFusionPluginStats = Objects.requireNonNull(stats);
    }

    public NativeExecutorsStats(StreamInput in) throws IOException {
        RuntimeValues ioRuntime = in.readOptionalWriteable(RuntimeValues::new);
        RuntimeValues cpuRuntime = in.readOptionalWriteable(RuntimeValues::new);
        TaskMonitorValues queryExecution = new TaskMonitorValues(in);
        TaskMonitorValues streamNext = new TaskMonitorValues(in);
        TaskMonitorValues fetchPhase = new TaskMonitorValues(in);
        TaskMonitorValues segmentStats = new TaskMonitorValues(in);
        TaskMonitorValues indexedQueryExecution = new TaskMonitorValues(in);
        this.dataFusionPluginStats = new DataFusionPluginStats(ioRuntime, cpuRuntime, queryExecution, streamNext, fetchPhase, segmentStats, indexedQueryExecution);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(dataFusionPluginStats.getIoRuntime());
        out.writeOptionalWriteable(dataFusionPluginStats.getCpuRuntime());
        dataFusionPluginStats.getQueryExecution().writeTo(out);
        dataFusionPluginStats.getStreamNext().writeTo(out);
        dataFusionPluginStats.getFetchPhase().writeTo(out);
        dataFusionPluginStats.getSegmentStats().writeTo(out);
        dataFusionPluginStats.getIndexedQueryExecution().writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        RuntimeValues ioRuntime = dataFusionPluginStats.getIoRuntime();
        if (ioRuntime != null) {
            builder.startObject("io_runtime");
            ioRuntime.toXContent(builder);
            builder.endObject();
        }
        RuntimeValues cpuRuntime = dataFusionPluginStats.getCpuRuntime();
        if (cpuRuntime != null) {
            builder.startObject("cpu_runtime");
            cpuRuntime.toXContent(builder);
            builder.endObject();
        }
        builder.startObject("task_monitors");
        builder.startObject("query_execution");
        dataFusionPluginStats.getQueryExecution().toXContent(builder);
        builder.endObject();
        builder.startObject("stream_next");
        dataFusionPluginStats.getStreamNext().toXContent(builder);
        builder.endObject();
        builder.startObject("fetch_phase");
        dataFusionPluginStats.getFetchPhase().toXContent(builder);
        builder.endObject();
        builder.startObject("segment_stats");
        dataFusionPluginStats.getSegmentStats().toXContent(builder);
        builder.endObject();
        builder.startObject("indexed_query_execution");
        dataFusionPluginStats.getIndexedQueryExecution().toXContent(builder);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public DataFusionPluginStats getDataFusionPluginStats() {
        return dataFusionPluginStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NativeExecutorsStats that = (NativeExecutorsStats) o;
        return Objects.equals(dataFusionPluginStats, that.dataFusionPluginStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataFusionPluginStats);
    }
}
