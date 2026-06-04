/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.be.datafusion.stats.NativeExecutorsStats.OperationType;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Randomized tests for {@link NativeExecutorsStats} Writeable round-trip.
 *
 * <p>For any valid {@code NativeExecutorsStats} containing IO + optional CPU {@code RuntimeMetrics}
 * (9 fields each) and 4 {@code TaskMonitorStats} (3 fields each), writing to {@code StreamOutput}
 * and reading back from {@code StreamInput} produces an object whose field values are identical to
 * the original.
 *
 * <p>Validates: Requirements 6.6
 */
public class NativeExecutorsStatsTests extends OpenSearchTestCase {

    private static final int TRIES = 100;

    // ---- Generators ----

    private long nonNegLong() {
        return randomLongBetween(0, Long.MAX_VALUE / 2);
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

    /** RuntimeMetrics with workersCount &gt; 0, marking the CPU runtime as present. */
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

    // ---- Writeable round-trip preserves all fields ----

    public void testWriteableRoundTripPreservesAllFieldsWithCpu() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            NativeExecutorsStats original = randomNativeExecutorsStatsWithCpu();

            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            NativeExecutorsStats deserialized = new NativeExecutorsStats(in);

            assertRuntimeMetricsEqual(original.getIoRuntime(), deserialized.getIoRuntime(), "io_runtime");

            assertNotNull("original CPU runtime must be present", original.getCpuRuntime());
            assertNotNull("deserialized CPU runtime must be present", deserialized.getCpuRuntime());
            assertRuntimeMetricsEqual(original.getCpuRuntime(), deserialized.getCpuRuntime(), "cpu_runtime");

            assertTaskMonitorsEqual(original.getTaskMonitors(), deserialized.getTaskMonitors());

            assertEquals("Full NativeExecutorsStats round-trip must produce equal object", original, deserialized);
        }
    }

    public void testWriteableRoundTripPreservesAllFieldsNoCpu() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            NativeExecutorsStats original = randomNativeExecutorsStatsNoCpu();

            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            NativeExecutorsStats deserialized = new NativeExecutorsStats(in);

            assertRuntimeMetricsEqual(original.getIoRuntime(), deserialized.getIoRuntime(), "io_runtime");

            assertEquals(
                "CPU runtime must be null in both original and deserialized",
                original.getCpuRuntime(),
                deserialized.getCpuRuntime()
            );

            assertTaskMonitorsEqual(original.getTaskMonitors(), deserialized.getTaskMonitors());

            assertEquals("Full NativeExecutorsStats round-trip must produce equal object", original, deserialized);
        }
    }

    // ---- Helpers ----

    private void assertRuntimeMetricsEqual(RuntimeMetrics expected, RuntimeMetrics actual, String label) {
        assertEquals(label + ".workers_count", expected.workersCount, actual.workersCount);
        assertEquals(label + ".total_polls_count", expected.totalPollsCount, actual.totalPollsCount);
        assertEquals(label + ".total_busy_duration_ms", expected.totalBusyDurationMs, actual.totalBusyDurationMs);
        assertEquals(label + ".total_overflow_count", expected.totalOverflowCount, actual.totalOverflowCount);
        assertEquals(label + ".global_queue_depth", expected.globalQueueDepth, actual.globalQueueDepth);
        assertEquals(label + ".blocking_queue_depth", expected.blockingQueueDepth, actual.blockingQueueDepth);
        assertEquals(label + ".num_alive_tasks", expected.numAliveTasks, actual.numAliveTasks);
        assertEquals(label + ".spawned_tasks_count", expected.spawnedTasksCount, actual.spawnedTasksCount);
    }

    private void assertTaskMonitorsEqual(Map<String, TaskMonitorStats> expected, Map<String, TaskMonitorStats> actual) {
        assertEquals("original must have exactly 4 task monitors", 4, expected.size());
        assertEquals("deserialized must have exactly 4 task monitors", 4, actual.size());

        for (OperationType opType : OperationType.values()) {
            TaskMonitorStats exp = expected.get(opType.key());
            TaskMonitorStats act = actual.get(opType.key());
            assertNotNull("original must contain " + opType.key(), exp);
            assertNotNull("deserialized must contain " + opType.key(), act);

            assertEquals(opType.key() + ".total_poll_duration_ms", exp.totalPollDurationMs, act.totalPollDurationMs);
            assertEquals(opType.key() + ".total_scheduled_duration_ms", exp.totalScheduledDurationMs, act.totalScheduledDurationMs);
            assertEquals(opType.key() + ".total_idle_duration_ms", exp.totalIdleDurationMs, act.totalIdleDurationMs);
        }
    }
}
