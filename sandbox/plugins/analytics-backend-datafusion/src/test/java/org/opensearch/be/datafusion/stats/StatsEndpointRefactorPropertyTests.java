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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Randomized tests for the stats-endpoint-refactor spec.
 *
 * <p>Validates that the flattened JSON serialization preserves all metric values,
 * CPU runtime conditional presence, and transport round-trip correctness.
 *
 * <p>Feature: stats-endpoint-refactor
 */
public class StatsEndpointRefactorPropertyTests extends OpenSearchTestCase {

    private static final int TRIES = 200;

    /** JSON field names for RuntimeMetrics in documented order (9 fields). */
    private static final String[] RUNTIME_FIELD_NAMES = {
        "workers_count",
        "total_polls_count",
        "total_busy_duration_ms",
        "total_overflow_count",
        "global_queue_depth",
        "blocking_queue_depth",
        "num_alive_tasks",
        "spawned_tasks_count",
        "total_local_queue_depth" };

    /** JSON field names for TaskMonitorStats in documented order (3 fields). */
    private static final String[] TASK_FIELD_NAMES = { "total_poll_duration_ms", "total_scheduled_duration_ms", "total_idle_duration_ms" };

    // ---- Object generators ----

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

    private NativeExecutorsStats randomNativeExecutorsStatsCpuPresent() {
        return new NativeExecutorsStats(randomRuntimeMetrics(), randomRuntimeMetricsWithPositiveWorkers(), randomTaskMonitors());
    }

    private NativeExecutorsStats randomNativeExecutorsStatsCpuAbsent() {
        return new NativeExecutorsStats(randomRuntimeMetrics(), null, randomTaskMonitors());
    }

    /** DataFusionStats with non-null NativeExecutorsStats (CPU present or absent). */
    private DataFusionStats randomDataFusionStats() {
        NativeExecutorsStats nes = randomBoolean() ? randomNativeExecutorsStatsCpuPresent() : randomNativeExecutorsStatsCpuAbsent();
        return new DataFusionStats(
            nes,
            new PartitionGateStats("datanode_gate", 12, 0, 0, 0, 0, 12),
            new PartitionGateStats("coordinator_gate", 12, 0, 0, 0, 0, 12),
            null
        );
    }

    // ---- Property 1: Flat JSON serialization preserves all metric values at top level ----

    public void testFlatJsonSerializationPreservesAllMetricValues() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            NativeExecutorsStats nes = randomNativeExecutorsStatsCpuPresent();
            Map<String, Object> root = renderNativeExecutorsToMap(nes);

            // Verify native_executors and task_monitors wrappers are absent
            assertFalse("native_executors wrapper must be absent", root.containsKey("native_executors"));
            assertFalse("task_monitors wrapper must be absent", root.containsKey("task_monitors"));

            // Verify io_runtime is a top-level key with all 9 fields
            Map<String, Object> ioRuntime = asMap(root.get("io_runtime"));
            assertNotNull("io_runtime must be present at top level", ioRuntime);
            verifyRuntimeFields(nes.getIoRuntime(), ioRuntime);

            // Verify cpu_runtime is a top-level key with all 9 fields (present case)
            Map<String, Object> cpuRuntime = asMap(root.get("cpu_runtime"));
            assertNotNull("cpu_runtime must be present at top level when non-null", cpuRuntime);
            verifyRuntimeFields(nes.getCpuRuntime(), cpuRuntime);

            // Verify each task monitor is a top-level key with correct fields
            for (OperationType opType : OperationType.values()) {
                Map<String, Object> monitor = asMap(root.get(opType.key()));
                assertNotNull(opType.key() + " must be present at top level", monitor);
                verifyTaskMonitorFields(nes.getTaskMonitors().get(opType.key()), monitor, opType.key());
            }
        }
    }

    public void testFlatJsonSerializationPreservesAllMetricValuesCpuAbsent() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            NativeExecutorsStats nes = randomNativeExecutorsStatsCpuAbsent();
            Map<String, Object> root = renderNativeExecutorsToMap(nes);

            // Verify native_executors and task_monitors wrappers are absent
            assertFalse("native_executors wrapper must be absent", root.containsKey("native_executors"));
            assertFalse("task_monitors wrapper must be absent", root.containsKey("task_monitors"));

            // Verify io_runtime is a top-level key with all 9 fields
            Map<String, Object> ioRuntime = asMap(root.get("io_runtime"));
            assertNotNull("io_runtime must be present at top level", ioRuntime);
            verifyRuntimeFields(nes.getIoRuntime(), ioRuntime);

            // cpu_runtime absent
            assertFalse("cpu_runtime must be absent when null", root.containsKey("cpu_runtime"));

            // Verify each task monitor is a top-level key with correct fields
            for (OperationType opType : OperationType.values()) {
                Map<String, Object> monitor = asMap(root.get(opType.key()));
                assertNotNull(opType.key() + " must be present at top level", monitor);
                verifyTaskMonitorFields(nes.getTaskMonitors().get(opType.key()), monitor, opType.key());
            }
        }
    }

    // ---- Property 2: CPU runtime conditional presence ----

    public void testCpuRuntimePresentWhenNonNull() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            NativeExecutorsStats nes = randomNativeExecutorsStatsCpuPresent();
            Map<String, Object> root = renderNativeExecutorsToMap(nes);

            assertTrue("cpu_runtime must be present when cpuRuntime is non-null", root.containsKey("cpu_runtime"));
            Map<String, Object> cpuRuntime = asMap(root.get("cpu_runtime"));
            verifyRuntimeFields(nes.getCpuRuntime(), cpuRuntime);
        }
    }

    public void testCpuRuntimeAbsentWhenNull() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            NativeExecutorsStats nes = randomNativeExecutorsStatsCpuAbsent();
            Map<String, Object> root = renderNativeExecutorsToMap(nes);

            assertFalse("cpu_runtime must be absent when cpuRuntime is null", root.containsKey("cpu_runtime"));
        }
    }

    // ---- Property 3: Transport serialization round-trip ----

    public void testTransportSerializationRoundTrip() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStats original = randomDataFusionStats();
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            DataFusionStats deserialized = new DataFusionStats(in);
            assertEquals("Transport round-trip must preserve all fields", original, deserialized);
        }
    }

    // ---- Helper methods ----

    @SuppressWarnings("unchecked")
    private Map<String, Object> renderNativeExecutorsToMap(NativeExecutorsStats nes) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        nes.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    private void verifyRuntimeFields(RuntimeMetrics rm, Map<String, Object> runtimeNode) {
        long[] expected = {
            rm.workersCount,
            rm.totalPollsCount,
            rm.totalBusyDurationMs,
            rm.totalOverflowCount,
            rm.globalQueueDepth,
            rm.blockingQueueDepth,
            rm.numAliveTasks,
            rm.spawnedTasksCount,
            rm.totalLocalQueueDepth };
        for (int i = 0; i < RUNTIME_FIELD_NAMES.length; i++) {
            String fieldName = RUNTIME_FIELD_NAMES[i];
            assertTrue("Runtime field '" + fieldName + "' must be present", runtimeNode.containsKey(fieldName));
            assertEquals(
                "Runtime field '" + fieldName + "': expected " + expected[i],
                expected[i],
                ((Number) runtimeNode.get(fieldName)).longValue()
            );
        }
    }

    private void verifyTaskMonitorFields(TaskMonitorStats tm, Map<String, Object> monitorNode, String opType) {
        long[] expected = { tm.totalPollDurationMs, tm.totalScheduledDurationMs, tm.totalIdleDurationMs };
        for (int i = 0; i < TASK_FIELD_NAMES.length; i++) {
            String fieldName = TASK_FIELD_NAMES[i];
            assertTrue(opType + " field '" + fieldName + "' must be present", monitorNode.containsKey(fieldName));
            assertEquals(
                opType + " field '" + fieldName + "': expected " + expected[i],
                expected[i],
                ((Number) monitorNode.get(fieldName)).longValue()
            );
        }
    }
}
