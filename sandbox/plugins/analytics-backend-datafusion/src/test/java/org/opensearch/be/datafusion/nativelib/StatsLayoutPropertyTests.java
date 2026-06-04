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
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Randomized tests for {@link StatsLayout} struct decode.
 *
 * <p>Validates the three correctness properties from the ffm-struct-layout design:
 * <ol>
 *   <li>Pack-then-decode round-trip preserves all fields</li>
 *   <li>Decode-then-reencode produces byte-identical buffer</li>
 *   <li>Writeable serialization round-trip</li>
 * </ol>
 */
public class StatsLayoutPropertyTests extends OpenSearchTestCase {

    private static final int TRIES = 100;

    private static final int FIELD_COUNT = 38;

    // ---- Generators ----

    private long nonNegLong() {
        return randomLongBetween(0, Long.MAX_VALUE / 2);
    }

    private long[] randomFieldArray() {
        long[] arr = new long[FIELD_COUNT];
        for (int i = 0; i < FIELD_COUNT; i++) {
            arr[i] = nonNegLong();
        }
        return arr;
    }

    private long[] randomFieldArrayWithCpuWorkersZero() {
        long[] arr = randomFieldArray();
        arr[9] = 0; // cpu_runtime.workers_count = 0
        return arr;
    }

    private long[] randomFieldArrayWithCpuWorkersPositive() {
        long[] arr = randomFieldArray();
        arr[9] = randomLongBetween(1, Long.MAX_VALUE / 2); // ensure cpu_runtime.workers_count > 0
        return arr;
    }

    private RuntimeMetrics randomRuntimeMetrics() {
        return new RuntimeMetrics(
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong()
        );
    }

    private RuntimeMetrics randomRuntimeMetricsWithPositiveWorkers() {
        return new RuntimeMetrics(
            randomLongBetween(1, Long.MAX_VALUE / 2),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong()
        );
    }

    private TaskMonitorStats randomTaskMonitorStats() {
        return new TaskMonitorStats(nonNegLong(), nonNegLong(), nonNegLong());
    }

    private Map<String, TaskMonitorStats> randomTaskMonitors() {
        Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
        monitors.put("coordinator_reduce", randomTaskMonitorStats());
        monitors.put("query_execution", randomTaskMonitorStats());
        monitors.put("stream_next", randomTaskMonitorStats());
        monitors.put("plan_setup", randomTaskMonitorStats());
        return monitors;
    }

    private NativeExecutorsStats randomNativeExecutorsStatsWithCpu() {
        return new NativeExecutorsStats(randomRuntimeMetrics(), randomRuntimeMetricsWithPositiveWorkers(), randomTaskMonitors());
    }

    private NativeExecutorsStats randomNativeExecutorsStatsNoCpu() {
        return new NativeExecutorsStats(randomRuntimeMetrics(), null, randomTaskMonitors());
    }

    // ---- Property 1: Pack-then-decode round-trip (cpu workers > 0) ----

    public void testPackThenDecodeRoundTripWithCpu() {
        for (int t = 0; t < TRIES; t++) {
            long[] values = randomFieldArrayWithCpuWorkersPositive();
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
                assertTrue("cpu workers should be > 0", cpuWorkers > 0);
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
                    assertEquals(tmGroups[g] + ".total_poll_duration_ms", values[base], tm.totalPollDurationMs);
                    assertEquals(tmGroups[g] + ".total_scheduled_duration_ms", values[base + 1], tm.totalScheduledDurationMs);
                    assertEquals(tmGroups[g] + ".total_idle_duration_ms", values[base + 2], tm.totalIdleDurationMs);
                }
            }
        }
    }

    public void testPackThenDecodeRoundTripCpuNull() {
        for (int t = 0; t < TRIES; t++) {
            long[] values = randomFieldArrayWithCpuWorkersZero();
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
                assertNull("cpuRuntime must be null when workers_count == 0", cpuRuntime);
            }
        }
    }

    // ---- Property 2: Decode-then-reencode identity ----

    public void testDecodeThenReencodeIdentity() {
        for (int t = 0; t < TRIES; t++) {
            long[] values = randomFieldArray();
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
                for (int i = 0; i < decoded.length; i++) {
                    reencoded.setAtIndex(ValueLayout.JAVA_LONG, i, decoded[i]);
                }

                // Compare byte-for-byte over the decoded prefix
                byte[] originalBytes = original.asSlice(0, (long) decoded.length * Long.BYTES).toArray(ValueLayout.JAVA_BYTE);
                byte[] reencodedBytes = reencoded.asSlice(0, (long) decoded.length * Long.BYTES).toArray(ValueLayout.JAVA_BYTE);
                assertArrayEquals("Decode-then-reencode must produce byte-identical buffer", originalBytes, reencodedBytes);
            }
        }
    }

    // ---- Property 3: Writeable serialization round-trip ----

    public void testWriteableRoundTripWithCpu() throws IOException {
        for (int t = 0; t < TRIES; t++) {
            NativeExecutorsStats original = randomNativeExecutorsStatsWithCpu();
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            NativeExecutorsStats deserialized = new NativeExecutorsStats(in);
            assertEquals("Writeable round-trip must produce equal object", original, deserialized);
        }
    }

    public void testWriteableRoundTripNoCpu() throws IOException {
        for (int t = 0; t < TRIES; t++) {
            NativeExecutorsStats original = randomNativeExecutorsStatsNoCpu();
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            NativeExecutorsStats deserialized = new NativeExecutorsStats(in);
            assertEquals("Writeable round-trip must produce equal object", original, deserialized);
            assertNull("CPU runtime must be null", deserialized.getCpuRuntime());
        }
    }
}
