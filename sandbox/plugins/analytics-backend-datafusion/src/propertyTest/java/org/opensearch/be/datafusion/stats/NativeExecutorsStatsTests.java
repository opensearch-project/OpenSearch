/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.be.datafusion.stats.NativeExecutorsStats.OperationType;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats.TaskMonitorStats;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Property-based tests for {@link NativeExecutorsStats} Writeable round-trip.
 *
 * <p>Verifies Property 2 from the stats-spi-refactor design:
 * For any valid {@code NativeExecutorsStats} object containing IO + optional CPU
 * {@code RuntimeMetrics} (8 fields each) and 4 {@code TaskMonitorStats} (3 fields each),
 * writing to {@code StreamOutput} and reading from {@code StreamInput} SHALL produce
 * an object where all field values are identical to the original.
 *
 * <p>Tag: Feature: stats-spi-refactor, Property 2: NativeExecutorsStats Writeable round-trip
 *
 * <p><b>Validates: Requirements 6.6</b>
 */
public class NativeExecutorsStatsTests {

    // ---- Generators ----

    @Provide
    Arbitrary<RuntimeMetrics> runtimeMetrics() {
        return Arbitraries.longs()
            .between(0, Long.MAX_VALUE / 2)
            .list()
            .ofSize(9)
            .map(l -> new RuntimeMetrics(l.get(0), l.get(1), l.get(2), l.get(3), l.get(4), l.get(5), l.get(6), l.get(7), l.get(8)));
    }

    @Provide
    Arbitrary<TaskMonitorStats> taskMonitorValues() {
        Arbitrary<Long> nonNeg = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(nonNeg, nonNeg, nonNeg).as(TaskMonitorStats::new);
    }

    @Provide
    Arbitrary<NativeExecutorsStats> nativeExecutorsStatsWithCpu() {
        return Combinators.combine(
            runtimeMetrics(),                // IO runtime
            runtimeMetrics().map(rt -> {     // CPU runtime (ensure workers_count > 0)
                if (rt.workersCount == 0) {
                    return new RuntimeMetrics(
                        1,
                        rt.totalPollsCount,
                        rt.totalBusyDurationMs,
                        rt.totalOverflowCount,
                        rt.globalQueueDepth,
                        rt.blockingQueueDepth,
                        rt.numAliveTasks,
                        rt.spawnedTasksCount,
                        rt.totalLocalQueueDepth
                    );
                }
                return rt;
            }),
            taskMonitorValues(),             // query_execution
            taskMonitorValues(),             // stream_next
            taskMonitorValues(),             // fetch_phase
            taskMonitorValues()              // segment_stats
        ).as((io, cpu, qe, sn, fp, ss) -> {
            Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
            monitors.put("query_execution", qe);
            monitors.put("stream_next", sn);
            monitors.put("fetch_phase", fp);
            monitors.put("segment_stats", ss);
            return new NativeExecutorsStats(io, cpu, monitors);
        });
    }

    @Provide
    Arbitrary<NativeExecutorsStats> nativeExecutorsStatsNoCpu() {
        return Combinators.combine(
            runtimeMetrics(),                // IO runtime
            taskMonitorValues(),             // query_execution
            taskMonitorValues(),             // stream_next
            taskMonitorValues(),             // fetch_phase
            taskMonitorValues()              // segment_stats
        ).as((io, qe, sn, fp, ss) -> {
            Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
            monitors.put("query_execution", qe);
            monitors.put("stream_next", sn);
            monitors.put("fetch_phase", fp);
            monitors.put("segment_stats", ss);
            return new NativeExecutorsStats(io, null, monitors);
        });
    }

    // ---- Property 2: Writeable round-trip preserves all fields ----

    /**
     * Property 2: Writeable round-trip preserves all fields (with CPU runtime present).
     *
     * <p>Tag: Feature: stats-spi-refactor, Property 2: NativeExecutorsStats Writeable round-trip
     *
     * <p><b>Validates: Requirements 6.6</b>
     */
    @Property(tries = 100)
    void writeableRoundTripPreservesAllFieldsWithCpu(@ForAll("nativeExecutorsStatsWithCpu") NativeExecutorsStats original)
        throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);

        assertRuntimeMetricsEqual(original.getIoRuntime(), deserialized.getIoRuntime(), "io_runtime");

        assertNotNull(original.getCpuRuntime(), "original CPU runtime must be present");
        assertNotNull(deserialized.getCpuRuntime(), "deserialized CPU runtime must be present");
        assertRuntimeMetricsEqual(original.getCpuRuntime(), deserialized.getCpuRuntime(), "cpu_runtime");

        assertTaskMonitorsEqual(original.getTaskMonitors(), deserialized.getTaskMonitors());

        assertEquals(original, deserialized, "Full NativeExecutorsStats round-trip must produce equal object");
    }

    /**
     * Property 2 (complement): Writeable round-trip preserves all fields (CPU runtime absent).
     *
     * <p>Tag: Feature: stats-spi-refactor, Property 2: NativeExecutorsStats Writeable round-trip
     *
     * <p><b>Validates: Requirements 6.6</b>
     */
    @Property(tries = 100)
    void writeableRoundTripPreservesAllFieldsNoCpu(@ForAll("nativeExecutorsStatsNoCpu") NativeExecutorsStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);

        assertRuntimeMetricsEqual(original.getIoRuntime(), deserialized.getIoRuntime(), "io_runtime");

        assertEquals(original.getCpuRuntime(), deserialized.getCpuRuntime(), "CPU runtime must be null in both original and deserialized");

        assertTaskMonitorsEqual(original.getTaskMonitors(), deserialized.getTaskMonitors());

        assertEquals(original, deserialized, "Full NativeExecutorsStats round-trip must produce equal object");
    }

    // ---- Helpers ----

    private void assertRuntimeMetricsEqual(RuntimeMetrics expected, RuntimeMetrics actual, String label) {
        assertEquals(expected.workersCount, actual.workersCount, label + ".workers_count");
        assertEquals(expected.totalPollsCount, actual.totalPollsCount, label + ".total_polls_count");
        assertEquals(expected.totalBusyDurationMs, actual.totalBusyDurationMs, label + ".total_busy_duration_ms");
        assertEquals(expected.totalOverflowCount, actual.totalOverflowCount, label + ".total_overflow_count");
        assertEquals(expected.globalQueueDepth, actual.globalQueueDepth, label + ".global_queue_depth");
        assertEquals(expected.blockingQueueDepth, actual.blockingQueueDepth, label + ".blocking_queue_depth");
        assertEquals(expected.numAliveTasks, actual.numAliveTasks, label + ".num_alive_tasks");
        assertEquals(expected.spawnedTasksCount, actual.spawnedTasksCount, label + ".spawned_tasks_count");
    }

    private void assertTaskMonitorsEqual(Map<String, TaskMonitorStats> expected, Map<String, TaskMonitorStats> actual) {
        assertEquals(4, expected.size(), "original must have exactly 4 task monitors");
        assertEquals(4, actual.size(), "deserialized must have exactly 4 task monitors");

        for (OperationType opType : OperationType.values()) {
            TaskMonitorStats exp = expected.get(opType.key());
            TaskMonitorStats act = actual.get(opType.key());
            assertNotNull(exp, "original must contain " + opType.key());
            assertNotNull(act, "deserialized must contain " + opType.key());

            assertEquals(exp.totalPollDurationMs, act.totalPollDurationMs, opType.key() + ".total_poll_duration_ms");
            assertEquals(exp.totalScheduledDurationMs, act.totalScheduledDurationMs, opType.key() + ".total_scheduled_duration_ms");
            assertEquals(exp.totalIdleDurationMs, act.totalIdleDurationMs, opType.key() + ".total_idle_duration_ms");
        }
    }
}
