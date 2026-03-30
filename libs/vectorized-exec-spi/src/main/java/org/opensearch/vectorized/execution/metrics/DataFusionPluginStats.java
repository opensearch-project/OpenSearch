/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import com.google.protobuf.InvalidProtocolBufferException;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.vectorized.execution.metrics.proto.DataFusionStatsProto;
import org.opensearch.vectorized.execution.metrics.proto.TokioMetricsProto;

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
     * Decodes protobuf bytes (from JNI) into a DataFusionPluginStats instance.
     *
     * @param bytes protobuf-encoded DataFusionStats message
     * @return a new DataFusionPluginStats instance
     * @throws IllegalArgumentException if the bytes cannot be decoded
     */
    public static DataFusionPluginStats decode(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Cannot decode null bytes for DataFusionStats");
        }
        try {
            DataFusionStatsProto.DataFusionStats proto = DataFusionStatsProto.DataFusionStats.parseFrom(bytes);

            RuntimeValues ioRuntime = proto.hasIoRuntime() ? RuntimeValues.fromProto(proto.getIoRuntime()) : null;
            RuntimeValues cpuRuntime = proto.hasCpuRuntime() ? RuntimeValues.fromProto(proto.getCpuRuntime()) : null;

            TaskMonitorValues queryExecution;
            TaskMonitorValues streamNext;
            TaskMonitorValues fetchPhase;
            TaskMonitorValues segmentStats;
            TaskMonitorValues indexedQueryExecution;

            if (proto.hasTaskMonitors()) {
                DataFusionStatsProto.TaskMonitors tm = proto.getTaskMonitors();
                queryExecution = tm.hasQueryExecution() ? TaskMonitorValues.fromProto(tm.getQueryExecution()) : TaskMonitorValues.EMPTY;
                streamNext = tm.hasStreamNext() ? TaskMonitorValues.fromProto(tm.getStreamNext()) : TaskMonitorValues.EMPTY;
                fetchPhase = tm.hasFetchPhase() ? TaskMonitorValues.fromProto(tm.getFetchPhase()) : TaskMonitorValues.EMPTY;
                segmentStats = tm.hasSegmentStats() ? TaskMonitorValues.fromProto(tm.getSegmentStats()) : TaskMonitorValues.EMPTY;
                indexedQueryExecution = tm.hasIndexedQueryExecution() ? TaskMonitorValues.fromProto(tm.getIndexedQueryExecution()) : TaskMonitorValues.EMPTY;
            } else {
                queryExecution = TaskMonitorValues.EMPTY;
                streamNext = TaskMonitorValues.EMPTY;
                fetchPhase = TaskMonitorValues.EMPTY;
                segmentStats = TaskMonitorValues.EMPTY;
                indexedQueryExecution = TaskMonitorValues.EMPTY;
            }

            return new DataFusionPluginStats(ioRuntime, cpuRuntime, queryExecution, streamNext, fetchPhase, segmentStats, indexedQueryExecution);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to decode DataFusionStats protobuf", e);
        }
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
     */
    public static class RuntimeValues implements Writeable {

        private final long workersCount;
        private final long totalPollsCount;
        private final long totalBusyDurationMs;
        private final long totalOverflowCount;
        private final long globalQueueDepth;
        private final long blockingQueueDepth;

        public RuntimeValues(
            long workersCount,
            long totalPollsCount,
            long totalBusyDurationMs,
            long totalOverflowCount,
            long globalQueueDepth,
            long blockingQueueDepth
        ) {
            this.workersCount = workersCount;
            this.totalPollsCount = totalPollsCount;
            this.totalBusyDurationMs = totalBusyDurationMs;
            this.totalOverflowCount = totalOverflowCount;
            this.globalQueueDepth = globalQueueDepth;
            this.blockingQueueDepth = blockingQueueDepth;
        }

        /**
         * Read from a stream.
         */
        public RuntimeValues(StreamInput in) throws IOException {
            this.workersCount = in.readVLong();
            this.totalPollsCount = in.readVLong();
            this.totalBusyDurationMs = in.readVLong();
            this.totalOverflowCount = in.readVLong();
            this.globalQueueDepth = in.readVLong();
            this.blockingQueueDepth = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(workersCount);
            out.writeVLong(totalPollsCount);
            out.writeVLong(totalBusyDurationMs);
            out.writeVLong(totalOverflowCount);
            out.writeVLong(globalQueueDepth);
            out.writeVLong(blockingQueueDepth);
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("workers_count", workersCount);
            builder.field("total_polls_count", totalPollsCount);
            builder.field("total_busy_duration_ms", totalBusyDurationMs);
            builder.field("total_overflow_count", totalOverflowCount);
            builder.field("global_queue_depth", globalQueueDepth);
            builder.field("blocking_queue_depth", blockingQueueDepth);
        }

        /**
         * Constructs a RuntimeValues from a protobuf RuntimeMetrics message.
         */
        static RuntimeValues fromProto(TokioMetricsProto.RuntimeMetrics rm) {
            return new RuntimeValues(
                rm.getWorkersCount(),
                rm.getTotalPollsCount(),
                rm.getTotalBusyDurationMs(),
                rm.getTotalOverflowCount(),
                rm.getGlobalQueueDepth(),
                rm.getBlockingQueueDepth()
            );
        }

        public long getWorkersCount() { return workersCount; }
        public long getTotalPollsCount() { return totalPollsCount; }
        public long getTotalBusyDurationMs() { return totalBusyDurationMs; }
        public long getTotalOverflowCount() { return totalOverflowCount; }
        public long getGlobalQueueDepth() { return globalQueueDepth; }
        public long getBlockingQueueDepth() { return blockingQueueDepth; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RuntimeValues that = (RuntimeValues) o;
            return workersCount == that.workersCount
                && totalPollsCount == that.totalPollsCount
                && totalBusyDurationMs == that.totalBusyDurationMs
                && totalOverflowCount == that.totalOverflowCount
                && globalQueueDepth == that.globalQueueDepth
                && blockingQueueDepth == that.blockingQueueDepth;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                workersCount, totalPollsCount, totalBusyDurationMs,
                totalOverflowCount, globalQueueDepth, blockingQueueDepth
            );
        }
    }

    /**
     * Holds the 3 actionable duration fields for a single task monitor.
     */
    public static class TaskMonitorValues implements Writeable {

        static final TaskMonitorValues EMPTY = new TaskMonitorValues(0, 0, 0);

        private final long totalPollDurationMs;
        private final long totalScheduledDurationMs;
        private final long totalIdleDurationMs;

        public TaskMonitorValues(
            long totalPollDurationMs,
            long totalScheduledDurationMs,
            long totalIdleDurationMs
        ) {
            this.totalPollDurationMs = totalPollDurationMs;
            this.totalScheduledDurationMs = totalScheduledDurationMs;
            this.totalIdleDurationMs = totalIdleDurationMs;
        }

        /**
         * Read from a stream.
         */
        public TaskMonitorValues(StreamInput in) throws IOException {
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

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("total_poll_duration_ms", totalPollDurationMs);
            builder.field("total_scheduled_duration_ms", totalScheduledDurationMs);
            builder.field("total_idle_duration_ms", totalIdleDurationMs);
        }

        /**
         * Constructs a TaskMonitorValues from a protobuf TaskMonitorMetrics message.
         */
        static TaskMonitorValues fromProto(TokioMetricsProto.TaskMonitorMetrics tm) {
            return new TaskMonitorValues(
                tm.getTotalPollDurationMs(),
                tm.getTotalScheduledDurationMs(),
                tm.getTotalIdleDurationMs()
            );
        }

        public long getTotalPollDurationMs() { return totalPollDurationMs; }
        public long getTotalScheduledDurationMs() { return totalScheduledDurationMs; }
        public long getTotalIdleDurationMs() { return totalIdleDurationMs; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskMonitorValues that = (TaskMonitorValues) o;
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
