/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Concrete stats for the DataFusion plugin.
 * Contains all runtime (IO + CPU) and task monitor metrics in one class.
 *
 * Lives in the SPI module so that server-side consumers (NodeStats, admission
 * control, thread pool stats) get typed field access directly — same pattern
 * as {@code OsStats} and {@code JvmStats}. No {@code NamedWriteableRegistry}
 * entry is needed.
 *
 * @opensearch.internal
 */
public class DataFusionPluginStats implements PluginStats {

    private static final String IO_RUNTIME = "io_runtime";
    private static final String CPU_RUNTIME = "cpu_runtime";
    private static final String TASK_MONITORS = "task_monitors";
    private static final String QUERY_EXECUTION = "query_execution";
    private static final String STREAM_NEXT = "stream_next";
    private static final String FETCH_PHASE = "fetch_phase";
    private static final String SEGMENT_STATS = "segment_stats";
    private static final String INDEXED_QUERY_EXECUTION = "indexed_query_execution";

    @Nullable
    private final RuntimeValues ioRuntime;
    @Nullable
    private final RuntimeValues cpuRuntime;
    private final TaskMonitorValues queryExecution;
    private final TaskMonitorValues streamNext;
    private final TaskMonitorValues fetchPhase;
    private final TaskMonitorValues segmentStats;
    private final TaskMonitorValues indexedQueryExecution;

    public DataFusionPluginStats(
        @Nullable RuntimeValues ioRuntime,
        @Nullable RuntimeValues cpuRuntime,
        TaskMonitorValues queryExecution,
        TaskMonitorValues streamNext,
        TaskMonitorValues fetchPhase,
        TaskMonitorValues segmentStats,
        TaskMonitorValues indexedQueryExecution
    ) {
        this.ioRuntime = ioRuntime;
        this.cpuRuntime = cpuRuntime;
        this.queryExecution = Objects.requireNonNull(queryExecution);
        this.streamNext = Objects.requireNonNull(streamNext);
        this.fetchPhase = Objects.requireNonNull(fetchPhase);
        this.segmentStats = Objects.requireNonNull(segmentStats);
        this.indexedQueryExecution = Objects.requireNonNull(indexedQueryExecution);
    }

    /**
     * Decodes a flat {@code long[]} array (from JNI) into a DataFusionPluginStats instance.
     * The array must contain exactly 27 elements laid out as:
     * [0..5] io_runtime, [6..11] cpu_runtime, [12..14] query_execution,
     * [15..17] stream_next, [18..20] fetch_phase, [21..23] segment_stats,
     * [24..26] indexed_query_execution.
     *
     * @param data flat long array of 27 elements
     * @return a new DataFusionPluginStats instance
     * @throws IllegalArgumentException if the array is null or not fully consumed
     */
    public static DataFusionPluginStats decode(long[] data) {
        if (data == null) {
            throw new IllegalArgumentException("Cannot decode null array for DataFusionStats");
        }
        ArrayCursor cursor = new ArrayCursor(data);
        RuntimeValues ioRuntime = cursor.read(RuntimeValues::new);
        RuntimeValues cpuRuntime = cursor.read(RuntimeValues::new);
        TaskMonitorValues queryExecution = cursor.read(TaskMonitorValues::new);
        TaskMonitorValues streamNext = cursor.read(TaskMonitorValues::new);
        TaskMonitorValues fetchPhase = cursor.read(TaskMonitorValues::new);
        TaskMonitorValues segmentStats = cursor.read(TaskMonitorValues::new);
        TaskMonitorValues indexedQueryExecution = cursor.read(TaskMonitorValues::new);
        cursor.assertFullyConsumed();
        RuntimeValues cpuOrNull = cpuRuntime.getWorkersCount() == 0 ? null : cpuRuntime;
        return new DataFusionPluginStats(ioRuntime, cpuOrNull, queryExecution, streamNext, fetchPhase, segmentStats, indexedQueryExecution);
    }

    @Nullable
    public RuntimeValues getIoRuntime() {
        return ioRuntime;
    }

    @Nullable
    public RuntimeValues getCpuRuntime() {
        return cpuRuntime;
    }

    public TaskMonitorValues getQueryExecution() {
        return queryExecution;
    }

    public TaskMonitorValues getStreamNext() {
        return streamNext;
    }

    public TaskMonitorValues getFetchPhase() {
        return fetchPhase;
    }

    public TaskMonitorValues getSegmentStats() {
        return segmentStats;
    }

    public TaskMonitorValues getIndexedQueryExecution() {
        return indexedQueryExecution;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionPluginStats that = (DataFusionPluginStats) o;
        return Objects.equals(ioRuntime, that.ioRuntime)
            && Objects.equals(cpuRuntime, that.cpuRuntime)
            && Objects.equals(queryExecution, that.queryExecution)
            && Objects.equals(streamNext, that.streamNext)
            && Objects.equals(fetchPhase, that.fetchPhase)
            && Objects.equals(segmentStats, that.segmentStats)
            && Objects.equals(indexedQueryExecution, that.indexedQueryExecution);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ioRuntime, cpuRuntime, queryExecution, streamNext, fetchPhase, segmentStats, indexedQueryExecution);
    }

    /**
     * Holds the 6 actionable fields from tokio RuntimeMetrics for a single runtime.
     * Extends {@link NativeStatsBlock} so it can be decoded from a positional {@code long[]} array.
     */
    public static class RuntimeValues extends NativeStatsBlock implements Writeable {

        /** Offset constants for positional access within this block. */
        public static final int WORKERS_COUNT = 0;
        public static final int TOTAL_POLLS_COUNT = 1;
        public static final int TOTAL_BUSY_DURATION_MS = 2;
        public static final int TOTAL_OVERFLOW_COUNT = 3;
        public static final int GLOBAL_QUEUE_DEPTH = 4;
        public static final int BLOCKING_QUEUE_DEPTH = 5;
        public static final int SIZE = 6;

        /**
         * Creates a RuntimeValues view over a slice of the given array.
         *
         * @param data   the source array (typically the full JNI payload)
         * @param offset start position within {@code data}
         */
        public RuntimeValues(long[] data, int offset) {
            super(data, offset, SIZE);
        }

        /**
         * Writeable deserialization constructor — keeps the existing 6-arg shape
         * so that callers using {@code new RuntimeValues(w, x, y, z, a, b)} still compile.
         */
        public RuntimeValues(
            long workersCount,
            long totalPollsCount,
            long totalBusyDurationMs,
            long totalOverflowCount,
            long globalQueueDepth,
            long blockingQueueDepth
        ) {
            super(
                new long[] { workersCount, totalPollsCount, totalBusyDurationMs, totalOverflowCount, globalQueueDepth, blockingQueueDepth },
                0,
                SIZE
            );
        }

        /**
         * Read from a stream.
         */
        public RuntimeValues(StreamInput in) throws IOException {
            super(
                new long[] { in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong() },
                0,
                SIZE
            );
        }

        @Override
        public int size() {
            return SIZE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(get(WORKERS_COUNT));
            out.writeVLong(get(TOTAL_POLLS_COUNT));
            out.writeVLong(get(TOTAL_BUSY_DURATION_MS));
            out.writeVLong(get(TOTAL_OVERFLOW_COUNT));
            out.writeVLong(get(GLOBAL_QUEUE_DEPTH));
            out.writeVLong(get(BLOCKING_QUEUE_DEPTH));
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("workers_count", get(WORKERS_COUNT));
            builder.field("total_polls_count", get(TOTAL_POLLS_COUNT));
            builder.field("total_busy_duration_ms", get(TOTAL_BUSY_DURATION_MS));
            builder.field("total_overflow_count", get(TOTAL_OVERFLOW_COUNT));
            builder.field("global_queue_depth", get(GLOBAL_QUEUE_DEPTH));
            builder.field("blocking_queue_depth", get(BLOCKING_QUEUE_DEPTH));
        }

        public long getWorkersCount() { return get(WORKERS_COUNT); }
        public long getTotalPollsCount() { return get(TOTAL_POLLS_COUNT); }
        public long getTotalBusyDurationMs() { return get(TOTAL_BUSY_DURATION_MS); }
        public long getTotalOverflowCount() { return get(TOTAL_OVERFLOW_COUNT); }
        public long getGlobalQueueDepth() { return get(GLOBAL_QUEUE_DEPTH); }
        public long getBlockingQueueDepth() { return get(BLOCKING_QUEUE_DEPTH); }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RuntimeValues that = (RuntimeValues) o;
            return get(WORKERS_COUNT) == that.get(WORKERS_COUNT)
                && get(TOTAL_POLLS_COUNT) == that.get(TOTAL_POLLS_COUNT)
                && get(TOTAL_BUSY_DURATION_MS) == that.get(TOTAL_BUSY_DURATION_MS)
                && get(TOTAL_OVERFLOW_COUNT) == that.get(TOTAL_OVERFLOW_COUNT)
                && get(GLOBAL_QUEUE_DEPTH) == that.get(GLOBAL_QUEUE_DEPTH)
                && get(BLOCKING_QUEUE_DEPTH) == that.get(BLOCKING_QUEUE_DEPTH);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                get(WORKERS_COUNT), get(TOTAL_POLLS_COUNT), get(TOTAL_BUSY_DURATION_MS),
                get(TOTAL_OVERFLOW_COUNT), get(GLOBAL_QUEUE_DEPTH), get(BLOCKING_QUEUE_DEPTH)
            );
        }
    }

    /**
     * Holds the 3 actionable duration fields for a single task monitor.
     * Extends {@link NativeStatsBlock} so it can be decoded from a positional {@code long[]} array.
     */
    public static class TaskMonitorValues extends NativeStatsBlock implements Writeable {

        /** Offset constants for positional access within this block. */
        public static final int TOTAL_POLL_DURATION_MS = 0;
        public static final int TOTAL_SCHEDULED_DURATION_MS = 1;
        public static final int TOTAL_IDLE_DURATION_MS = 2;
        public static final int SIZE = 3;

        static final TaskMonitorValues EMPTY = new TaskMonitorValues(0, 0, 0);

        /**
         * Creates a TaskMonitorValues view over a slice of the given array.
         *
         * @param data   the source array (typically the full JNI payload)
         * @param offset start position within {@code data}
         */
        public TaskMonitorValues(long[] data, int offset) {
            super(data, offset, SIZE);
        }

        /**
         * Writeable deserialization constructor — keeps the existing 3-arg shape
         * so that callers using {@code new TaskMonitorValues(a, b, c)} still compile.
         */
        public TaskMonitorValues(
            long totalPollDurationMs,
            long totalScheduledDurationMs,
            long totalIdleDurationMs
        ) {
            super(new long[] { totalPollDurationMs, totalScheduledDurationMs, totalIdleDurationMs }, 0, SIZE);
        }

        /**
         * Read from a stream.
         */
        public TaskMonitorValues(StreamInput in) throws IOException {
            super(new long[] { in.readVLong(), in.readVLong(), in.readVLong() }, 0, SIZE);
        }

        @Override
        public int size() {
            return SIZE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(get(TOTAL_POLL_DURATION_MS));
            out.writeVLong(get(TOTAL_SCHEDULED_DURATION_MS));
            out.writeVLong(get(TOTAL_IDLE_DURATION_MS));
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("total_poll_duration_ms", get(TOTAL_POLL_DURATION_MS));
            builder.field("total_scheduled_duration_ms", get(TOTAL_SCHEDULED_DURATION_MS));
            builder.field("total_idle_duration_ms", get(TOTAL_IDLE_DURATION_MS));
        }

        public long getTotalPollDurationMs() { return get(TOTAL_POLL_DURATION_MS); }
        public long getTotalScheduledDurationMs() { return get(TOTAL_SCHEDULED_DURATION_MS); }
        public long getTotalIdleDurationMs() { return get(TOTAL_IDLE_DURATION_MS); }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskMonitorValues that = (TaskMonitorValues) o;
            return get(TOTAL_POLL_DURATION_MS) == that.get(TOTAL_POLL_DURATION_MS)
                && get(TOTAL_SCHEDULED_DURATION_MS) == that.get(TOTAL_SCHEDULED_DURATION_MS)
                && get(TOTAL_IDLE_DURATION_MS) == that.get(TOTAL_IDLE_DURATION_MS);
        }

        @Override
        public int hashCode() {
            return Objects.hash(get(TOTAL_POLL_DURATION_MS), get(TOTAL_SCHEDULED_DURATION_MS), get(TOTAL_IDLE_DURATION_MS));
        }
    }
}
