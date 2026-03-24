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
     * Holds all 28 fields from tokio_metrics::RuntimeMetrics for a single runtime.
     */
    public static class RuntimeValues implements Writeable {

        private final long workersCount;
        private final long totalParkCount;
        private final long maxParkCount;
        private final long minParkCount;
        private final long totalNoopCount;
        private final long maxNoopCount;
        private final long minNoopCount;
        private final long totalStealCount;
        private final long maxStealCount;
        private final long minStealCount;
        private final long totalStealOperations;
        private final long totalLocalScheduleCount;
        private final long maxLocalScheduleCount;
        private final long minLocalScheduleCount;
        private final long totalOverflowCount;
        private final long maxOverflowCount;
        private final long minOverflowCount;
        private final long totalPollsCount;
        private final long maxPollsCount;
        private final long minPollsCount;
        private final long totalBusyDurationMs;
        private final long maxBusyDurationMs;
        private final long minBusyDurationMs;
        private final long totalLocalQueueDepth;
        private final long maxLocalQueueDepth;
        private final long minLocalQueueDepth;
        private final long globalQueueDepth;
        private final long blockingQueueDepth;

        public RuntimeValues(
            long workersCount, long totalParkCount, long maxParkCount, long minParkCount,
            long totalNoopCount, long maxNoopCount, long minNoopCount,
            long totalStealCount, long maxStealCount, long minStealCount, long totalStealOperations,
            long totalLocalScheduleCount, long maxLocalScheduleCount, long minLocalScheduleCount,
            long totalOverflowCount, long maxOverflowCount, long minOverflowCount,
            long totalPollsCount, long maxPollsCount, long minPollsCount,
            long totalBusyDurationMs, long maxBusyDurationMs, long minBusyDurationMs,
            long totalLocalQueueDepth, long maxLocalQueueDepth, long minLocalQueueDepth,
            long globalQueueDepth, long blockingQueueDepth
        ) {
            this.workersCount = workersCount;
            this.totalParkCount = totalParkCount;
            this.maxParkCount = maxParkCount;
            this.minParkCount = minParkCount;
            this.totalNoopCount = totalNoopCount;
            this.maxNoopCount = maxNoopCount;
            this.minNoopCount = minNoopCount;
            this.totalStealCount = totalStealCount;
            this.maxStealCount = maxStealCount;
            this.minStealCount = minStealCount;
            this.totalStealOperations = totalStealOperations;
            this.totalLocalScheduleCount = totalLocalScheduleCount;
            this.maxLocalScheduleCount = maxLocalScheduleCount;
            this.minLocalScheduleCount = minLocalScheduleCount;
            this.totalOverflowCount = totalOverflowCount;
            this.maxOverflowCount = maxOverflowCount;
            this.minOverflowCount = minOverflowCount;
            this.totalPollsCount = totalPollsCount;
            this.maxPollsCount = maxPollsCount;
            this.minPollsCount = minPollsCount;
            this.totalBusyDurationMs = totalBusyDurationMs;
            this.maxBusyDurationMs = maxBusyDurationMs;
            this.minBusyDurationMs = minBusyDurationMs;
            this.totalLocalQueueDepth = totalLocalQueueDepth;
            this.maxLocalQueueDepth = maxLocalQueueDepth;
            this.minLocalQueueDepth = minLocalQueueDepth;
            this.globalQueueDepth = globalQueueDepth;
            this.blockingQueueDepth = blockingQueueDepth;
        }

        /**
         * Read from a stream.
         */
        public RuntimeValues(StreamInput in) throws IOException {
            this.workersCount = in.readVLong();
            this.totalParkCount = in.readVLong();
            this.maxParkCount = in.readVLong();
            this.minParkCount = in.readVLong();
            this.totalNoopCount = in.readVLong();
            this.maxNoopCount = in.readVLong();
            this.minNoopCount = in.readVLong();
            this.totalStealCount = in.readVLong();
            this.maxStealCount = in.readVLong();
            this.minStealCount = in.readVLong();
            this.totalStealOperations = in.readVLong();
            this.totalLocalScheduleCount = in.readVLong();
            this.maxLocalScheduleCount = in.readVLong();
            this.minLocalScheduleCount = in.readVLong();
            this.totalOverflowCount = in.readVLong();
            this.maxOverflowCount = in.readVLong();
            this.minOverflowCount = in.readVLong();
            this.totalPollsCount = in.readVLong();
            this.maxPollsCount = in.readVLong();
            this.minPollsCount = in.readVLong();
            this.totalBusyDurationMs = in.readVLong();
            this.maxBusyDurationMs = in.readVLong();
            this.minBusyDurationMs = in.readVLong();
            this.totalLocalQueueDepth = in.readVLong();
            this.maxLocalQueueDepth = in.readVLong();
            this.minLocalQueueDepth = in.readVLong();
            this.globalQueueDepth = in.readVLong();
            this.blockingQueueDepth = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(workersCount);
            out.writeVLong(totalParkCount);
            out.writeVLong(maxParkCount);
            out.writeVLong(minParkCount);
            out.writeVLong(totalNoopCount);
            out.writeVLong(maxNoopCount);
            out.writeVLong(minNoopCount);
            out.writeVLong(totalStealCount);
            out.writeVLong(maxStealCount);
            out.writeVLong(minStealCount);
            out.writeVLong(totalStealOperations);
            out.writeVLong(totalLocalScheduleCount);
            out.writeVLong(maxLocalScheduleCount);
            out.writeVLong(minLocalScheduleCount);
            out.writeVLong(totalOverflowCount);
            out.writeVLong(maxOverflowCount);
            out.writeVLong(minOverflowCount);
            out.writeVLong(totalPollsCount);
            out.writeVLong(maxPollsCount);
            out.writeVLong(minPollsCount);
            out.writeVLong(totalBusyDurationMs);
            out.writeVLong(maxBusyDurationMs);
            out.writeVLong(minBusyDurationMs);
            out.writeVLong(totalLocalQueueDepth);
            out.writeVLong(maxLocalQueueDepth);
            out.writeVLong(minLocalQueueDepth);
            out.writeVLong(globalQueueDepth);
            out.writeVLong(blockingQueueDepth);
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("workers_count", workersCount);
            builder.field("total_park_count", totalParkCount);
            builder.field("max_park_count", maxParkCount);
            builder.field("min_park_count", minParkCount);
            builder.field("total_noop_count", totalNoopCount);
            builder.field("max_noop_count", maxNoopCount);
            builder.field("min_noop_count", minNoopCount);
            builder.field("total_steal_count", totalStealCount);
            builder.field("max_steal_count", maxStealCount);
            builder.field("min_steal_count", minStealCount);
            builder.field("total_steal_operations", totalStealOperations);
            builder.field("total_local_schedule_count", totalLocalScheduleCount);
            builder.field("max_local_schedule_count", maxLocalScheduleCount);
            builder.field("min_local_schedule_count", minLocalScheduleCount);
            builder.field("total_overflow_count", totalOverflowCount);
            builder.field("max_overflow_count", maxOverflowCount);
            builder.field("min_overflow_count", minOverflowCount);
            builder.field("total_polls_count", totalPollsCount);
            builder.field("max_polls_count", maxPollsCount);
            builder.field("min_polls_count", minPollsCount);
            builder.field("total_busy_duration_ms", totalBusyDurationMs);
            builder.field("max_busy_duration_ms", maxBusyDurationMs);
            builder.field("min_busy_duration_ms", minBusyDurationMs);
            builder.field("total_local_queue_depth", totalLocalQueueDepth);
            builder.field("max_local_queue_depth", maxLocalQueueDepth);
            builder.field("min_local_queue_depth", minLocalQueueDepth);
            builder.field("global_queue_depth", globalQueueDepth);
            builder.field("blocking_queue_depth", blockingQueueDepth);
        }

        /**
         * Constructs a RuntimeValues from a protobuf RuntimeMetrics message.
         */
        static RuntimeValues fromProto(TokioMetricsProto.RuntimeMetrics rm) {
            return new RuntimeValues(
                rm.getWorkersCount(), rm.getTotalParkCount(), rm.getMaxParkCount(), rm.getMinParkCount(),
                rm.getTotalNoopCount(), rm.getMaxNoopCount(), rm.getMinNoopCount(),
                rm.getTotalStealCount(), rm.getMaxStealCount(), rm.getMinStealCount(), rm.getTotalStealOperations(),
                rm.getTotalLocalScheduleCount(), rm.getMaxLocalScheduleCount(), rm.getMinLocalScheduleCount(),
                rm.getTotalOverflowCount(), rm.getMaxOverflowCount(), rm.getMinOverflowCount(),
                rm.getTotalPollsCount(), rm.getMaxPollsCount(), rm.getMinPollsCount(),
                rm.getTotalBusyDurationMs(), rm.getMaxBusyDurationMs(), rm.getMinBusyDurationMs(),
                rm.getTotalLocalQueueDepth(), rm.getMaxLocalQueueDepth(), rm.getMinLocalQueueDepth(),
                rm.getGlobalQueueDepth(), rm.getBlockingQueueDepth()
            );
        }

        public long getWorkersCount() { return workersCount; }
        public long getTotalParkCount() { return totalParkCount; }
        public long getMaxParkCount() { return maxParkCount; }
        public long getMinParkCount() { return minParkCount; }
        public long getTotalNoopCount() { return totalNoopCount; }
        public long getMaxNoopCount() { return maxNoopCount; }
        public long getMinNoopCount() { return minNoopCount; }
        public long getTotalStealCount() { return totalStealCount; }
        public long getMaxStealCount() { return maxStealCount; }
        public long getMinStealCount() { return minStealCount; }
        public long getTotalStealOperations() { return totalStealOperations; }
        public long getTotalLocalScheduleCount() { return totalLocalScheduleCount; }
        public long getMaxLocalScheduleCount() { return maxLocalScheduleCount; }
        public long getMinLocalScheduleCount() { return minLocalScheduleCount; }
        public long getTotalOverflowCount() { return totalOverflowCount; }
        public long getMaxOverflowCount() { return maxOverflowCount; }
        public long getMinOverflowCount() { return minOverflowCount; }
        public long getTotalPollsCount() { return totalPollsCount; }
        public long getMaxPollsCount() { return maxPollsCount; }
        public long getMinPollsCount() { return minPollsCount; }
        public long getTotalBusyDurationMs() { return totalBusyDurationMs; }
        public long getMaxBusyDurationMs() { return maxBusyDurationMs; }
        public long getMinBusyDurationMs() { return minBusyDurationMs; }
        public long getTotalLocalQueueDepth() { return totalLocalQueueDepth; }
        public long getMaxLocalQueueDepth() { return maxLocalQueueDepth; }
        public long getMinLocalQueueDepth() { return minLocalQueueDepth; }
        public long getGlobalQueueDepth() { return globalQueueDepth; }
        public long getBlockingQueueDepth() { return blockingQueueDepth; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RuntimeValues that = (RuntimeValues) o;
            return workersCount == that.workersCount
                && totalParkCount == that.totalParkCount && maxParkCount == that.maxParkCount && minParkCount == that.minParkCount
                && totalNoopCount == that.totalNoopCount && maxNoopCount == that.maxNoopCount && minNoopCount == that.minNoopCount
                && totalStealCount == that.totalStealCount && maxStealCount == that.maxStealCount && minStealCount == that.minStealCount
                && totalStealOperations == that.totalStealOperations
                && totalLocalScheduleCount == that.totalLocalScheduleCount
                && maxLocalScheduleCount == that.maxLocalScheduleCount && minLocalScheduleCount == that.minLocalScheduleCount
                && totalOverflowCount == that.totalOverflowCount
                && maxOverflowCount == that.maxOverflowCount && minOverflowCount == that.minOverflowCount
                && totalPollsCount == that.totalPollsCount && maxPollsCount == that.maxPollsCount && minPollsCount == that.minPollsCount
                && totalBusyDurationMs == that.totalBusyDurationMs
                && maxBusyDurationMs == that.maxBusyDurationMs && minBusyDurationMs == that.minBusyDurationMs
                && totalLocalQueueDepth == that.totalLocalQueueDepth
                && maxLocalQueueDepth == that.maxLocalQueueDepth && minLocalQueueDepth == that.minLocalQueueDepth
                && globalQueueDepth == that.globalQueueDepth && blockingQueueDepth == that.blockingQueueDepth;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                workersCount, totalParkCount, maxParkCount, minParkCount,
                totalNoopCount, maxNoopCount, minNoopCount,
                totalStealCount, maxStealCount, minStealCount, totalStealOperations,
                totalLocalScheduleCount, maxLocalScheduleCount, minLocalScheduleCount,
                totalOverflowCount, maxOverflowCount, minOverflowCount,
                totalPollsCount, maxPollsCount, minPollsCount,
                totalBusyDurationMs, maxBusyDurationMs, minBusyDurationMs,
                totalLocalQueueDepth, maxLocalQueueDepth, minLocalQueueDepth,
                globalQueueDepth, blockingQueueDepth
            );
        }
    }

    /**
     * Holds 6 fields for a single task monitor: 5 longs + 1 double (slowPollRatio).
     */
    public static class TaskMonitorValues implements Writeable {

        static final TaskMonitorValues EMPTY = new TaskMonitorValues(0, 0, 0, 0, 0, 0.0);

        private final long totalPollDurationMs;
        private final long totalScheduledDurationMs;
        private final long totalIdleDurationMs;
        private final long totalSlowPollCount;
        private final long totalLongDelayCount;
        private final double slowPollRatio;

        public TaskMonitorValues(
            long totalPollDurationMs,
            long totalScheduledDurationMs,
            long totalIdleDurationMs,
            long totalSlowPollCount,
            long totalLongDelayCount,
            double slowPollRatio
        ) {
            this.totalPollDurationMs = totalPollDurationMs;
            this.totalScheduledDurationMs = totalScheduledDurationMs;
            this.totalIdleDurationMs = totalIdleDurationMs;
            this.totalSlowPollCount = totalSlowPollCount;
            this.totalLongDelayCount = totalLongDelayCount;
            this.slowPollRatio = slowPollRatio;
        }

        /**
         * Read from a stream.
         */
        public TaskMonitorValues(StreamInput in) throws IOException {
            this.totalPollDurationMs = in.readVLong();
            this.totalScheduledDurationMs = in.readVLong();
            this.totalIdleDurationMs = in.readVLong();
            this.totalSlowPollCount = in.readVLong();
            this.totalLongDelayCount = in.readVLong();
            this.slowPollRatio = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalPollDurationMs);
            out.writeVLong(totalScheduledDurationMs);
            out.writeVLong(totalIdleDurationMs);
            out.writeVLong(totalSlowPollCount);
            out.writeVLong(totalLongDelayCount);
            out.writeDouble(slowPollRatio);
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("total_poll_duration_ms", totalPollDurationMs);
            builder.field("total_scheduled_duration_ms", totalScheduledDurationMs);
            builder.field("total_idle_duration_ms", totalIdleDurationMs);
            builder.field("total_slow_poll_count", totalSlowPollCount);
            builder.field("total_long_delay_count", totalLongDelayCount);
            builder.field("slow_poll_ratio", slowPollRatio);
        }

        /**
         * Constructs a TaskMonitorValues from a protobuf TaskMonitorMetrics message.
         */
        static TaskMonitorValues fromProto(TokioMetricsProto.TaskMonitorMetrics tm) {
            return new TaskMonitorValues(
                tm.getTotalPollDurationMs(),
                tm.getTotalScheduledDurationMs(),
                tm.getTotalIdleDurationMs(),
                tm.getTotalSlowPollCount(),
                tm.getTotalLongDelayCount(),
                tm.getSlowPollRatio()
            );
        }

        public long getTotalPollDurationMs() { return totalPollDurationMs; }
        public long getTotalScheduledDurationMs() { return totalScheduledDurationMs; }
        public long getTotalIdleDurationMs() { return totalIdleDurationMs; }
        public long getTotalSlowPollCount() { return totalSlowPollCount; }
        public long getTotalLongDelayCount() { return totalLongDelayCount; }
        public double getSlowPollRatio() { return slowPollRatio; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskMonitorValues that = (TaskMonitorValues) o;
            return totalPollDurationMs == that.totalPollDurationMs
                && totalScheduledDurationMs == that.totalScheduledDurationMs
                && totalIdleDurationMs == that.totalIdleDurationMs
                && totalSlowPollCount == that.totalSlowPollCount
                && totalLongDelayCount == that.totalLongDelayCount
                && Double.compare(that.slowPollRatio, slowPollRatio) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                totalPollDurationMs, totalScheduledDurationMs, totalIdleDurationMs,
                totalSlowPollCount, totalLongDelayCount, slowPollRatio
            );
        }
    }
}
