/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Defines the {@code MemoryLayout.structLayout} mirroring the Rust {@code DfStatsBuffer}
 * and provides {@link VarHandle} accessors for each field via layout path navigation.
 *
 * <p>The layout contains 6 named groups (2 runtime × 9 fields + 4 task monitor × 3 fields = 30 longs = 240 bytes).
 */
public final class StatsLayout {

    private static final String[] RUNTIME_FIELDS = {
        "workers_count",
        "total_polls_count",
        "total_busy_duration_ms",
        "total_overflow_count",
        "global_queue_depth",
        "blocking_queue_depth",
        "num_alive_tasks",
        "spawned_tasks_count",
        "total_local_queue_depth" };

    private static final String[] TASK_MONITOR_FIELDS = {
        "total_poll_duration_ms",
        "total_scheduled_duration_ms",
        "total_idle_duration_ms" };

    /** The struct layout mirroring Rust's {@code DfStatsBuffer}. */
    public static final StructLayout LAYOUT = MemoryLayout.structLayout(
        runtimeGroup("io_runtime"),
        runtimeGroup("cpu_runtime"),
        taskMonitorGroup("coordinator_reduce"),
        taskMonitorGroup("query_execution"),
        taskMonitorGroup("stream_next"),
        taskMonitorGroup("plan_setup")
    );

    static {
        if (LAYOUT.byteSize() != 30 * Long.BYTES) {
            throw new AssertionError("StatsLayout size mismatch: expected " + (30 * Long.BYTES) + " but got " + LAYOUT.byteSize());
        }
    }

    // ---- VarHandles for io_runtime fields ----
    private static final VarHandle IO_WORKERS_COUNT = handle("io_runtime", "workers_count");
    private static final VarHandle IO_TOTAL_POLLS_COUNT = handle("io_runtime", "total_polls_count");
    private static final VarHandle IO_TOTAL_BUSY_DURATION_MS = handle("io_runtime", "total_busy_duration_ms");
    private static final VarHandle IO_TOTAL_OVERFLOW_COUNT = handle("io_runtime", "total_overflow_count");
    private static final VarHandle IO_GLOBAL_QUEUE_DEPTH = handle("io_runtime", "global_queue_depth");
    private static final VarHandle IO_BLOCKING_QUEUE_DEPTH = handle("io_runtime", "blocking_queue_depth");
    private static final VarHandle IO_NUM_ALIVE_TASKS = handle("io_runtime", "num_alive_tasks");
    private static final VarHandle IO_SPAWNED_TASKS_COUNT = handle("io_runtime", "spawned_tasks_count");
    private static final VarHandle IO_TOTAL_LOCAL_QUEUE_DEPTH = handle("io_runtime", "total_local_queue_depth");

    // ---- VarHandles for cpu_runtime fields ----
    private static final VarHandle CPU_WORKERS_COUNT = handle("cpu_runtime", "workers_count");
    private static final VarHandle CPU_TOTAL_POLLS_COUNT = handle("cpu_runtime", "total_polls_count");
    private static final VarHandle CPU_TOTAL_BUSY_DURATION_MS = handle("cpu_runtime", "total_busy_duration_ms");
    private static final VarHandle CPU_TOTAL_OVERFLOW_COUNT = handle("cpu_runtime", "total_overflow_count");
    private static final VarHandle CPU_GLOBAL_QUEUE_DEPTH = handle("cpu_runtime", "global_queue_depth");
    private static final VarHandle CPU_BLOCKING_QUEUE_DEPTH = handle("cpu_runtime", "blocking_queue_depth");
    private static final VarHandle CPU_NUM_ALIVE_TASKS = handle("cpu_runtime", "num_alive_tasks");
    private static final VarHandle CPU_SPAWNED_TASKS_COUNT = handle("cpu_runtime", "spawned_tasks_count");
    private static final VarHandle CPU_TOTAL_LOCAL_QUEUE_DEPTH = handle("cpu_runtime", "total_local_queue_depth");

    // ---- VarHandles for coordinator_reduce fields ----
    private static final VarHandle CR_TOTAL_POLL_DURATION_MS = handle("coordinator_reduce", "total_poll_duration_ms");
    private static final VarHandle CR_TOTAL_SCHEDULED_DURATION_MS = handle("coordinator_reduce", "total_scheduled_duration_ms");
    private static final VarHandle CR_TOTAL_IDLE_DURATION_MS = handle("coordinator_reduce", "total_idle_duration_ms");

    // ---- VarHandles for query_execution fields ----
    private static final VarHandle QE_TOTAL_POLL_DURATION_MS = handle("query_execution", "total_poll_duration_ms");
    private static final VarHandle QE_TOTAL_SCHEDULED_DURATION_MS = handle("query_execution", "total_scheduled_duration_ms");
    private static final VarHandle QE_TOTAL_IDLE_DURATION_MS = handle("query_execution", "total_idle_duration_ms");

    // ---- VarHandles for stream_next fields ----
    private static final VarHandle SN_TOTAL_POLL_DURATION_MS = handle("stream_next", "total_poll_duration_ms");
    private static final VarHandle SN_TOTAL_SCHEDULED_DURATION_MS = handle("stream_next", "total_scheduled_duration_ms");
    private static final VarHandle SN_TOTAL_IDLE_DURATION_MS = handle("stream_next", "total_idle_duration_ms");

    // ---- VarHandles for plan_setup fields ----
    private static final VarHandle PS_TOTAL_POLL_DURATION_MS = handle("plan_setup", "total_poll_duration_ms");
    private static final VarHandle PS_TOTAL_SCHEDULED_DURATION_MS = handle("plan_setup", "total_scheduled_duration_ms");
    private static final VarHandle PS_TOTAL_IDLE_DURATION_MS = handle("plan_setup", "total_idle_duration_ms");

    private StatsLayout() {}

    /**
     * Read a single field from the segment.
     *
     * @param seg   the memory segment containing the DfStatsBuffer
     * @param group the group name (e.g. "io_runtime", "cpu_runtime")
     * @param field the field name (e.g. "workers_count")
     * @return the long value at the specified path
     */
    public static long readField(MemorySegment seg, String group, String field) {
        return (long) handle(group, field).get(seg, 0L);
    }

    /**
     * Read a runtime metrics group (9 fields) from the segment.
     *
     * @param seg   the memory segment containing the DfStatsBuffer
     * @param group "io_runtime" or "cpu_runtime"
     * @return a populated RuntimeMetrics instance
     */
    public static RuntimeMetrics readRuntimeMetrics(MemorySegment seg, String group) {
        VarHandle[] handles = runtimeHandles(group);
        return new RuntimeMetrics(
            (long) handles[0].get(seg, 0L),
            (long) handles[1].get(seg, 0L),
            (long) handles[2].get(seg, 0L),
            (long) handles[3].get(seg, 0L),
            (long) handles[4].get(seg, 0L),
            (long) handles[5].get(seg, 0L),
            (long) handles[6].get(seg, 0L),
            (long) handles[7].get(seg, 0L),
            (long) handles[8].get(seg, 0L)
        );
    }

    /**
     * Read a task monitor group (3 fields) from the segment.
     *
     * @param seg   the memory segment containing the DfStatsBuffer
     * @param group one of the OperationType keys
     * @return a populated TaskMonitorStats instance
     */
    public static TaskMonitorStats readTaskMonitor(MemorySegment seg, String group) {
        VarHandle[] handles = taskMonitorHandles(group);
        return new TaskMonitorStats((long) handles[0].get(seg, 0L), (long) handles[1].get(seg, 0L), (long) handles[2].get(seg, 0L));
    }

    // ---- Private helpers ----

    private static StructLayout runtimeGroup(String name) {
        return MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("workers_count"),
            ValueLayout.JAVA_LONG.withName("total_polls_count"),
            ValueLayout.JAVA_LONG.withName("total_busy_duration_ms"),
            ValueLayout.JAVA_LONG.withName("total_overflow_count"),
            ValueLayout.JAVA_LONG.withName("global_queue_depth"),
            ValueLayout.JAVA_LONG.withName("blocking_queue_depth"),
            ValueLayout.JAVA_LONG.withName("num_alive_tasks"),
            ValueLayout.JAVA_LONG.withName("spawned_tasks_count"),
            ValueLayout.JAVA_LONG.withName("total_local_queue_depth")
        ).withName(name);
    }

    private static StructLayout taskMonitorGroup(String name) {
        return MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("total_poll_duration_ms"),
            ValueLayout.JAVA_LONG.withName("total_scheduled_duration_ms"),
            ValueLayout.JAVA_LONG.withName("total_idle_duration_ms")
        ).withName(name);
    }

    private static VarHandle handle(String group, String field) {
        return LAYOUT.varHandle(PathElement.groupElement(group), PathElement.groupElement(field));
    }

    private static VarHandle[] runtimeHandles(String group) {
        return switch (group) {
            case "io_runtime" -> new VarHandle[] {
                IO_WORKERS_COUNT,
                IO_TOTAL_POLLS_COUNT,
                IO_TOTAL_BUSY_DURATION_MS,
                IO_TOTAL_OVERFLOW_COUNT,
                IO_GLOBAL_QUEUE_DEPTH,
                IO_BLOCKING_QUEUE_DEPTH,
                IO_NUM_ALIVE_TASKS,
                IO_SPAWNED_TASKS_COUNT,
                IO_TOTAL_LOCAL_QUEUE_DEPTH };
            case "cpu_runtime" -> new VarHandle[] {
                CPU_WORKERS_COUNT,
                CPU_TOTAL_POLLS_COUNT,
                CPU_TOTAL_BUSY_DURATION_MS,
                CPU_TOTAL_OVERFLOW_COUNT,
                CPU_GLOBAL_QUEUE_DEPTH,
                CPU_BLOCKING_QUEUE_DEPTH,
                CPU_NUM_ALIVE_TASKS,
                CPU_SPAWNED_TASKS_COUNT,
                CPU_TOTAL_LOCAL_QUEUE_DEPTH };
            default -> throw new IllegalArgumentException("Unknown runtime group: " + group);
        };
    }

    private static VarHandle[] taskMonitorHandles(String group) {
        return switch (group) {
            case "coordinator_reduce" -> new VarHandle[] {
                CR_TOTAL_POLL_DURATION_MS,
                CR_TOTAL_SCHEDULED_DURATION_MS,
                CR_TOTAL_IDLE_DURATION_MS };
            case "query_execution" -> new VarHandle[] {
                QE_TOTAL_POLL_DURATION_MS,
                QE_TOTAL_SCHEDULED_DURATION_MS,
                QE_TOTAL_IDLE_DURATION_MS };
            case "stream_next" -> new VarHandle[] { SN_TOTAL_POLL_DURATION_MS, SN_TOTAL_SCHEDULED_DURATION_MS, SN_TOTAL_IDLE_DURATION_MS };
            case "plan_setup" -> new VarHandle[] { PS_TOTAL_POLL_DURATION_MS, PS_TOTAL_SCHEDULED_DURATION_MS, PS_TOTAL_IDLE_DURATION_MS };
            default -> throw new IllegalArgumentException("Unknown task monitor group: " + group);
        };
    }
}
