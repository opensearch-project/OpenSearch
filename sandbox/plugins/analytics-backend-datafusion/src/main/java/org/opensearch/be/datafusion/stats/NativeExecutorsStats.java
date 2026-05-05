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
 * query_execution, stream_next, fetch_phase, segment_stats.
 */
public class NativeExecutorsStats implements Writeable, ToXContentFragment {

    /** Operation types in documented order. */
    public enum OperationType {
        /** Query execution operation. */
        QUERY_EXECUTION("query_execution"),
        /** Stream next (pagination) operation. */
        STREAM_NEXT("stream_next"),
        /** Fetch phase operation. */
        FETCH_PHASE("fetch_phase"),
        /** Segment-level statistics collection operation. */
        SEGMENT_STATS("segment_stats");

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
        builder.startObject("native_executors");

        builder.startObject("io_runtime");
        ioRuntime.toXContent(builder);
        builder.endObject();

        if (cpuRuntime != null) {
            builder.startObject("cpu_runtime");
            cpuRuntime.toXContent(builder);
            builder.endObject();
        }

        builder.startObject("task_monitors");
        for (Map.Entry<String, TaskMonitorStats> entry : taskMonitors.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder);
            builder.endObject();
        }
        builder.endObject(); // task_monitors

        builder.endObject(); // native_executors
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

    /**
     * 8 fields from {@code tokio_metrics::RuntimeMonitor} describing
     * per-worker thread pool behavior for a single Tokio runtime.
     */
    public static class RuntimeMetrics implements Writeable {
        /** Number of worker threads in the runtime. */
        public final long workersCount;
        /** Total number of task polls across all workers. */
        public final long totalPollsCount;
        /** Total time workers spent executing tasks, in milliseconds. */
        public final long totalBusyDurationMs;
        /** Total number of times tasks were pushed to the overflow queue. */
        public final long totalOverflowCount;
        /** Current depth of the global injection queue. */
        public final long globalQueueDepth;
        /** Current depth of the blocking thread pool queue. */
        public final long blockingQueueDepth;
        /** Number of tasks currently alive (spawned but not yet completed) on this runtime. */
        public final long numAliveTasks;
        /** Total number of tasks spawned on this runtime since creation. */
        public final long spawnedTasksCount;
        /** Sum of all per-worker local queue depths (tasks queued on worker-local run queues). */
        public final long totalLocalQueueDepth;

        /**
         * Construct from explicit field values.
         *
         * @param workersCount        number of worker threads
         * @param totalPollsCount     total task polls across all workers
         * @param totalBusyDurationMs total busy time in milliseconds
         * @param totalOverflowCount  total overflow queue pushes
         * @param globalQueueDepth    current global injection queue depth
         * @param blockingQueueDepth  current blocking thread pool queue depth
         * @param numAliveTasks       tasks currently alive
         * @param spawnedTasksCount   total tasks spawned since creation
         * @param totalLocalQueueDepth     sum of per-worker local queue depths
         */
        public RuntimeMetrics(
            long workersCount,
            long totalPollsCount,
            long totalBusyDurationMs,
            long totalOverflowCount,
            long globalQueueDepth,
            long blockingQueueDepth,
            long numAliveTasks,
            long spawnedTasksCount,
            long totalLocalQueueDepth
        ) {
            this.workersCount = workersCount;
            this.totalPollsCount = totalPollsCount;
            this.totalBusyDurationMs = totalBusyDurationMs;
            this.totalOverflowCount = totalOverflowCount;
            this.globalQueueDepth = globalQueueDepth;
            this.blockingQueueDepth = blockingQueueDepth;
            this.numAliveTasks = numAliveTasks;
            this.spawnedTasksCount = spawnedTasksCount;
            this.totalLocalQueueDepth = totalLocalQueueDepth;
        }

        /**
         * Deserialize from stream.
         *
         * @param in the stream input
         * @throws IOException if deserialization fails
         */
        public RuntimeMetrics(StreamInput in) throws IOException {
            this.workersCount = in.readVLong();
            this.totalPollsCount = in.readVLong();
            this.totalBusyDurationMs = in.readVLong();
            this.totalOverflowCount = in.readVLong();
            this.globalQueueDepth = in.readVLong();
            this.blockingQueueDepth = in.readVLong();
            this.numAliveTasks = in.readVLong();
            this.spawnedTasksCount = in.readVLong();
            this.totalLocalQueueDepth = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(workersCount);
            out.writeVLong(totalPollsCount);
            out.writeVLong(totalBusyDurationMs);
            out.writeVLong(totalOverflowCount);
            out.writeVLong(globalQueueDepth);
            out.writeVLong(blockingQueueDepth);
            out.writeVLong(numAliveTasks);
            out.writeVLong(spawnedTasksCount);
            out.writeVLong(totalLocalQueueDepth);
        }

        /**
         * Render all 8 fields as snake_case JSON fields.
         *
         * @param builder the XContent builder to write to
         * @throws IOException if writing fails
         */
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("workers_count", workersCount);
            builder.field("total_polls_count", totalPollsCount);
            builder.field("total_busy_duration_ms", totalBusyDurationMs);
            builder.field("total_overflow_count", totalOverflowCount);
            builder.field("global_queue_depth", globalQueueDepth);
            builder.field("blocking_queue_depth", blockingQueueDepth);
            builder.field("num_alive_tasks", numAliveTasks);
            builder.field("spawned_tasks_count", spawnedTasksCount);
            builder.field("total_local_queue_depth", totalLocalQueueDepth);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RuntimeMetrics that = (RuntimeMetrics) o;
            return workersCount == that.workersCount
                && totalPollsCount == that.totalPollsCount
                && totalBusyDurationMs == that.totalBusyDurationMs
                && totalOverflowCount == that.totalOverflowCount
                && globalQueueDepth == that.globalQueueDepth
                && blockingQueueDepth == that.blockingQueueDepth
                && numAliveTasks == that.numAliveTasks
                && spawnedTasksCount == that.spawnedTasksCount
                && totalLocalQueueDepth == that.totalLocalQueueDepth;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                workersCount,
                totalPollsCount,
                totalBusyDurationMs,
                totalOverflowCount,
                globalQueueDepth,
                blockingQueueDepth,
                numAliveTasks,
                spawnedTasksCount,
                totalLocalQueueDepth
            );
        }
    }

    /**
     * 3 duration fields per operation type from {@code tokio_metrics::TaskMonitor::cumulative()}.
     */
    public static class TaskMonitorStats implements Writeable {
        /** Total time spent polling instrumented futures, in milliseconds. */
        public final long totalPollDurationMs;
        /** Total time tasks spent waiting in the scheduler queue, in milliseconds. */
        public final long totalScheduledDurationMs;
        /** Total time tasks spent idle between polls, in milliseconds. */
        public final long totalIdleDurationMs;

        /**
         * Construct from explicit field values.
         *
         * @param totalPollDurationMs      total poll duration in milliseconds
         * @param totalScheduledDurationMs total scheduled duration in milliseconds
         * @param totalIdleDurationMs      total idle duration in milliseconds
         */
        public TaskMonitorStats(long totalPollDurationMs, long totalScheduledDurationMs, long totalIdleDurationMs) {
            this.totalPollDurationMs = totalPollDurationMs;
            this.totalScheduledDurationMs = totalScheduledDurationMs;
            this.totalIdleDurationMs = totalIdleDurationMs;
        }

        /**
         * Deserialize from stream.
         *
         * @param in the stream input
         * @throws IOException if deserialization fails
         */
        public TaskMonitorStats(StreamInput in) throws IOException {
            this.totalPollDurationMs = in.readVLong();
            this.totalScheduledDurationMs = in.readVLong();
            this.totalIdleDurationMs = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalPollDurationMs);
            out.writeVLong(totalScheduledDurationMs);
            out.writeVLong(totalIdleDurationMs);
        }

        /**
         * Render all 3 fields as snake_case JSON fields.
         *
         * @param builder the XContent builder to write to
         * @throws IOException if writing fails
         */
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("total_poll_duration_ms", totalPollDurationMs);
            builder.field("total_scheduled_duration_ms", totalScheduledDurationMs);
            builder.field("total_idle_duration_ms", totalIdleDurationMs);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskMonitorStats that = (TaskMonitorStats) o;
            return totalPollDurationMs == that.totalPollDurationMs
                && totalScheduledDurationMs == that.totalScheduledDurationMs
                && totalIdleDurationMs == that.totalIdleDurationMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalPollDurationMs, totalScheduledDurationMs, totalIdleDurationMs);
        }
    }
}
