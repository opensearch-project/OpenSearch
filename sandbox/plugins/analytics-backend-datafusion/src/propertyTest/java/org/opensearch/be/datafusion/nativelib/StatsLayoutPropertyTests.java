/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.util.LinkedHashMap;
import java.util.Map;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Property-based tests for {@link StatsLayout} struct decode.
 *
 * <p>Validates the three correctness properties from the ffm-struct-layout design:
 * <ol>
 *   <li>Pack-then-decode round-trip preserves all fields</li>
 *   <li>Decode-then-reencode produces byte-identical buffer</li>
 *   <li>Writeable serialization round-trip</li>
 * </ol>
 */
public class StatsLayoutPropertyTests {

    private static final int FIELD_COUNT = 30;
    private static final int BUFFER_SIZE = FIELD_COUNT * Long.BYTES;

    // ---- Generators ----

    @Provide
    Arbitrary<long[]> thirtyLongs() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2).array(long[].class).ofSize(FIELD_COUNT);
    }

    @Provide
    Arbitrary<long[]> thirtyLongsWithCpuWorkersZero() {
        return thirtyLongs().map(arr -> {
            arr[9] = 0; // cpu_runtime.workers_count = 0
            return arr;
        });
    }

    @Provide
    Arbitrary<long[]> thirtyLongsWithCpuWorkersPositive() {
        return thirtyLongs().map(arr -> {
            if (arr[9] == 0) arr[9] = 1; // ensure cpu_runtime.workers_count > 0
            return arr;
        });
    }

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
        Arbitrary<RuntimeMetrics> cpuArb = runtimeMetrics().map(rt -> {
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
        });
        return Combinators.combine(
            runtimeMetrics(),
            cpuArb,
            taskMonitorValues(),
            taskMonitorValues(),
            taskMonitorValues(),
            taskMonitorValues()
        ).as((io, cpu, cr, qe, sn, ps) -> {
            Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
            monitors.put("coordinator_reduce", cr);
            monitors.put("query_execution", qe);
            monitors.put("stream_next", sn);
            monitors.put("plan_setup", ps);
            return new NativeExecutorsStats(io, cpu, monitors);
        });
    }

    @Provide
    Arbitrary<NativeExecutorsStats> nativeExecutorsStatsNoCpu() {
        return Combinators.combine(runtimeMetrics(), taskMonitorValues(), taskMonitorValues(), taskMonitorValues(), taskMonitorValues())
            .as((io, cr, qe, sn, ps) -> {
                Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
                monitors.put("coordinator_reduce", cr);
                monitors.put("query_execution", qe);
                monitors.put("stream_next", sn);
                monitors.put("plan_setup", ps);
                return new NativeExecutorsStats(io, null, monitors);
            });
    }

    // ---- Property 1: Pack-then-decode round-trip (cpu workers > 0) ----

    /**
     * Property 1: Pack-then-decode round-trip preserves all fields (CPU runtime present).
     *
     * Validates: Requirements 3.3, 3.4, 4.3, 4.4, 4.5, 4.6, 6.1, 8.1, 8.3, 8.4
     */
    @Property(tries = 100)
    @Tag("Feature: ffm-struct-layout, Property 1: Pack-then-decode round-trip preserves all fields")
    void packThenDecodeRoundTripWithCpu(@ForAll("thirtyLongsWithCpuWorkersPositive") long[] values) {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            for (int i = 0; i < FIELD_COUNT; i++) {
                seg.setAtIndex(ValueLayout.JAVA_LONG, i, values[i]);
            }

            var ioRuntime = StatsLayout.readRuntimeMetrics(seg, "io_runtime");
            assertEquals(values[0], ioRuntime.workersCount);
            assertEquals(values[1], ioRuntime.totalPollsCount);
            assertEquals(values[2], ioRuntime.totalBusyDurationMs);
            assertEquals(values[3], ioRuntime.totalOverflowCount);
            assertEquals(values[4], ioRuntime.globalQueueDepth);
            assertEquals(values[5], ioRuntime.blockingQueueDepth);
            assertEquals(values[6], ioRuntime.numAliveTasks);
            assertEquals(values[7], ioRuntime.spawnedTasksCount);
            assertEquals(values[8], ioRuntime.totalLocalQueueDepth);

            long cpuWorkers = StatsLayout.readField(seg, "cpu_runtime", "workers_count");
            assert cpuWorkers > 0 : "cpu workers should be > 0";
            var cpuRuntime = StatsLayout.readRuntimeMetrics(seg, "cpu_runtime");
            assertNotNull(cpuRuntime);
            assertEquals(values[9], cpuRuntime.workersCount);
            assertEquals(values[10], cpuRuntime.totalPollsCount);
            assertEquals(values[11], cpuRuntime.totalBusyDurationMs);
            assertEquals(values[12], cpuRuntime.totalOverflowCount);
            assertEquals(values[13], cpuRuntime.globalQueueDepth);
            assertEquals(values[14], cpuRuntime.blockingQueueDepth);
            assertEquals(values[15], cpuRuntime.numAliveTasks);
            assertEquals(values[16], cpuRuntime.spawnedTasksCount);
            assertEquals(values[17], cpuRuntime.totalLocalQueueDepth);

            String[] tmGroups = { "coordinator_reduce", "query_execution", "stream_next", "plan_setup" };
            for (int g = 0; g < 4; g++) {
                var tm = StatsLayout.readTaskMonitor(seg, tmGroups[g]);
                int base = 18 + g * 3;
                assertEquals(values[base], tm.totalPollDurationMs, tmGroups[g] + ".total_poll_duration_ms");
                assertEquals(values[base + 1], tm.totalScheduledDurationMs, tmGroups[g] + ".total_scheduled_duration_ms");
                assertEquals(values[base + 2], tm.totalIdleDurationMs, tmGroups[g] + ".total_idle_duration_ms");
            }
        }
    }

    /**
     * Property 1: Pack-then-decode round-trip — CPU runtime null when workers_count == 0.
     *
     * Validates: Requirements 3.3, 3.4, 4.4, 8.3
     */
    @Property(tries = 100)
    @Tag("Feature: ffm-struct-layout, Property 1: Pack-then-decode round-trip preserves all fields")
    void packThenDecodeRoundTripCpuNull(@ForAll("thirtyLongsWithCpuWorkersZero") long[] values) {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            for (int i = 0; i < FIELD_COUNT; i++) {
                seg.setAtIndex(ValueLayout.JAVA_LONG, i, values[i]);
            }

            long cpuWorkers = StatsLayout.readField(seg, "cpu_runtime", "workers_count");
            assertEquals(0L, cpuWorkers);

            // Simulate NativeBridge logic: null when workers_count == 0
            RuntimeMetrics cpuRuntime = null;
            if (cpuWorkers > 0) {
                cpuRuntime = StatsLayout.readRuntimeMetrics(seg, "cpu_runtime");
            }
            assertNull(cpuRuntime, "cpuRuntime must be null when workers_count == 0");
        }
    }

    // ---- Property 2: Decode-then-reencode identity ----

    /**
     * Property 2: Decode-then-reencode produces byte-identical buffer.
     *
     * Validates: Requirements 8.2
     */
    @Property(tries = 100)
    @Tag("Feature: ffm-struct-layout, Property 2: Decode-then-reencode produces byte-identical buffer")
    void decodeThenReencodeIdentity(@ForAll("thirtyLongs") long[] values) {
        try (var arena = Arena.ofConfined()) {
            // Write original values
            var original = arena.allocate(StatsLayout.LAYOUT);
            for (int i = 0; i < FIELD_COUNT; i++) {
                original.setAtIndex(ValueLayout.JAVA_LONG, i, values[i]);
            }

            // Decode all fields
            var ioRuntime = StatsLayout.readRuntimeMetrics(original, "io_runtime");
            var cpuRuntime = StatsLayout.readRuntimeMetrics(original, "cpu_runtime");
            var cr = StatsLayout.readTaskMonitor(original, "coordinator_reduce");
            var qe = StatsLayout.readTaskMonitor(original, "query_execution");
            var sn = StatsLayout.readTaskMonitor(original, "stream_next");
            var ps = StatsLayout.readTaskMonitor(original, "plan_setup");

            // Re-encode into new buffer
            var reencoded = arena.allocate(StatsLayout.LAYOUT);
            long[] decoded = {
                ioRuntime.workersCount,
                ioRuntime.totalPollsCount,
                ioRuntime.totalBusyDurationMs,
                ioRuntime.totalOverflowCount,
                ioRuntime.globalQueueDepth,
                ioRuntime.blockingQueueDepth,
                ioRuntime.numAliveTasks,
                ioRuntime.spawnedTasksCount,
                ioRuntime.totalLocalQueueDepth,
                cpuRuntime.workersCount,
                cpuRuntime.totalPollsCount,
                cpuRuntime.totalBusyDurationMs,
                cpuRuntime.totalOverflowCount,
                cpuRuntime.globalQueueDepth,
                cpuRuntime.blockingQueueDepth,
                cpuRuntime.numAliveTasks,
                cpuRuntime.spawnedTasksCount,
                cpuRuntime.totalLocalQueueDepth,
                cr.totalPollDurationMs,
                cr.totalScheduledDurationMs,
                cr.totalIdleDurationMs,
                qe.totalPollDurationMs,
                qe.totalScheduledDurationMs,
                qe.totalIdleDurationMs,
                sn.totalPollDurationMs,
                sn.totalScheduledDurationMs,
                sn.totalIdleDurationMs,
                ps.totalPollDurationMs,
                ps.totalScheduledDurationMs,
                ps.totalIdleDurationMs };
            for (int i = 0; i < FIELD_COUNT; i++) {
                reencoded.setAtIndex(ValueLayout.JAVA_LONG, i, decoded[i]);
            }

            // Compare byte-for-byte
            byte[] originalBytes = original.toArray(ValueLayout.JAVA_BYTE);
            byte[] reencodedBytes = reencoded.toArray(ValueLayout.JAVA_BYTE);
            assertArrayEquals(originalBytes, reencodedBytes, "Decode-then-reencode must produce byte-identical buffer");
        }
    }

    // ---- Property 3: Writeable serialization round-trip ----

    /**
     * Property 3: Writeable serialization round-trip (with CPU runtime).
     *
     * Validates: Requirements 6.2, 6.3
     */
    @Property(tries = 100)
    @Tag("Feature: ffm-struct-layout, Property 3: Writeable serialization round-trip")
    void writeableRoundTripWithCpu(@ForAll("nativeExecutorsStatsWithCpu") NativeExecutorsStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);
        assertEquals(original, deserialized, "Writeable round-trip must produce equal object");
    }

    /**
     * Property 3: Writeable serialization round-trip (CPU runtime absent).
     *
     * Validates: Requirements 6.2, 6.3
     */
    @Property(tries = 100)
    @Tag("Feature: ffm-struct-layout, Property 3: Writeable serialization round-trip")
    void writeableRoundTripNoCpu(@ForAll("nativeExecutorsStatsNoCpu") NativeExecutorsStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);
        assertEquals(original, deserialized, "Writeable round-trip must produce equal object");
        assertNull(deserialized.getCpuRuntime(), "CPU runtime must be null");
    }
}
