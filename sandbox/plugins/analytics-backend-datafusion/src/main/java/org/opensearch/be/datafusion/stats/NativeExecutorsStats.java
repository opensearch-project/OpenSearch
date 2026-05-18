/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link Writeable} + {@link ToXContentFragment} container for native executor metrics
 * (Tokio runtime metrics + per-operation task monitors).
 *
 * <p>Contains an IO {@link RuntimeMetrics} (always present), an optional CPU
 * {@link RuntimeMetrics}, and 4 {@link TaskMonitorStats} for the operation types:
 * coordinator_reduce, query_execution, stream_next, plan_setup.
 */
public class NativeExecutorsStats implements Writeable, ToXContentFragment {

    /** Operation types in documented order. */
    public enum OperationType {
        /** Coordinator-side local plan execution (reduce phase). */
        COORDINATOR_REDUCE("coordinator_reduce"),
        /** Query execution operation. */
        QUERY_EXECUTION("query_execution"),
        /** Stream next (pagination) operation. */
        STREAM_NEXT("stream_next"),
        /** Plan setup: session context creation + plan preparation. */
        PLAN_SETUP("plan_setup");

        private final String key;

        OperationType(String key) {
            this.key = key;
        }

        /** Returns the snake_case key used in serialization and XContent output. */
        public String key() {
            return key;
        }
    }

    private final RuntimeMetrics ioRuntime;
    private final RuntimeMetrics cpuRuntime; // nullable
    private final Map<String, TaskMonitorStats> taskMonitors;

    /**
     * Construct from individual components.
     *
     * @param ioRuntime    the IO runtime metrics (must not be null)
     * @param cpuRuntime   the CPU runtime metrics (nullable)
     * @param taskMonitors per-operation task monitor metrics
     */
    // cpuRuntime is nullable — zeroed when absent (workers_count == 0), omitted from XContent when null
    public NativeExecutorsStats(RuntimeMetrics ioRuntime, RuntimeMetrics cpuRuntime, Map<String, TaskMonitorStats> taskMonitors) {
        this.ioRuntime = Objects.requireNonNull(ioRuntime);
        this.cpuRuntime = cpuRuntime;
        this.taskMonitors = Objects.requireNonNull(taskMonitors);
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public NativeExecutorsStats(StreamInput in) throws IOException {
        this.ioRuntime = new RuntimeMetrics(in);
        this.cpuRuntime = in.readBoolean() ? new RuntimeMetrics(in) : null;

        this.taskMonitors = new LinkedHashMap<>();
        for (OperationType opType : OperationType.values()) {
            this.taskMonitors.put(opType.key(), new TaskMonitorStats(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ioRuntime.writeTo(out);
        if (cpuRuntime != null) {
            out.writeBoolean(true);
            cpuRuntime.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        for (OperationType opType : OperationType.values()) {
            taskMonitors.get(opType.key()).writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("io_runtime");
        ioRuntime.toXContent(builder);
        builder.endObject();

        if (cpuRuntime != null) {
            builder.startObject("cpu_runtime");
            cpuRuntime.toXContent(builder);
            builder.endObject();
        }

        for (Map.Entry<String, TaskMonitorStats> entry : taskMonitors.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder);
            builder.endObject();
        }
        return builder;
    }

    /** Returns the IO runtime metrics. */
    public RuntimeMetrics getIoRuntime() {
        return ioRuntime;
    }

    /** Returns the CPU runtime metrics, or {@code null} if absent. */
    public RuntimeMetrics getCpuRuntime() {
        return cpuRuntime;
    }

    /** Returns the per-operation task monitor metrics. */
    public Map<String, TaskMonitorStats> getTaskMonitors() {
        return taskMonitors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NativeExecutorsStats that = (NativeExecutorsStats) o;
        return Objects.equals(ioRuntime, that.ioRuntime)
            && Objects.equals(cpuRuntime, that.cpuRuntime)
            && Objects.equals(taskMonitors, that.taskMonitors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ioRuntime, cpuRuntime, taskMonitors);
    }

}
