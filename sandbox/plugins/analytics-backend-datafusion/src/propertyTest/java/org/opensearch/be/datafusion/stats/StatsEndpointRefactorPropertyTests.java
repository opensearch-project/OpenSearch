/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.be.datafusion.stats.NativeExecutorsStats.OperationType;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for the stats-endpoint-refactor spec.
 *
 * <p>Validates that the flattened JSON serialization preserves all metric values,
 * CPU runtime conditional presence, and transport round-trip correctness.
 *
 * <p>Tag: Feature: stats-endpoint-refactor
 */
public class StatsEndpointRefactorPropertyTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    @Provide
    Arbitrary<RuntimeMetrics> runtimeMetrics() {
        return Arbitraries.longs()
            .between(0, Long.MAX_VALUE / 2)
            .list()
            .ofSize(9)
            .map(l -> new RuntimeMetrics(l.get(0), l.get(1), l.get(2), l.get(3), l.get(4), l.get(5), l.get(6), l.get(7), l.get(8)));
    }

    @Provide
    Arbitrary<TaskMonitorStats> taskMonitorStats() {
        Arbitrary<Long> nonNeg = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(nonNeg, nonNeg, nonNeg).as(TaskMonitorStats::new);
    }

    /** NativeExecutorsStats with CPU runtime present (workersCount > 0). */
    @Provide
    Arbitrary<NativeExecutorsStats> nativeExecutorsStatsCpuPresent() {
        return Combinators.combine(runtimeMetrics(), runtimeMetrics().map(rt -> {
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
        }), taskMonitorStats(), taskMonitorStats(), taskMonitorStats()).as((io, cpu, qe, sn, fp) -> {
            Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
            monitors.put("query_execution", qe);
            monitors.put("stream_next", sn);
            monitors.put("fetch_phase", fp);
            return new NativeExecutorsStats(io, cpu, monitors);
        });
    }

    /** NativeExecutorsStats with CPU runtime absent (null). */
    @Provide
    Arbitrary<NativeExecutorsStats> nativeExecutorsStatsCpuAbsent() {
        return Combinators.combine(runtimeMetrics(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats()).as((io, qe, sn, fp) -> {
            Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
            monitors.put("query_execution", qe);
            monitors.put("stream_next", sn);
            monitors.put("fetch_phase", fp);
            return new NativeExecutorsStats(io, null, monitors);
        });
    }

    /** DataFusionStats with non-null NativeExecutorsStats (CPU present or absent). */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStats() {
        return Arbitraries.oneOf(
            nativeExecutorsStatsCpuPresent().map(DataFusionStats::new),
            nativeExecutorsStatsCpuAbsent().map(DataFusionStats::new)
        );
    }

    // ---- Property 1: Flat JSON serialization preserves all metric values at top level ----

    /**
     * Feature: stats-endpoint-refactor, Property 1: Flat JSON serialization preserves all metric values at top level.
     *
     * <p>For any valid NativeExecutorsStats, toXContent produces JSON with io_runtime, each task monitor,
     * and optionally cpu_runtime as direct top-level keys with correct field values, and native_executors
     * and task_monitors keys are absent.
     *
     * <p>Validates: Requirements 2.1, 2.4, 2.5, 3.1, 3.2
     */
    @Property(tries = 200)
    void flatJsonSerializationPreservesAllMetricValues(@ForAll("nativeExecutorsStatsCpuPresent") NativeExecutorsStats nes)
        throws IOException {
        String json = renderNativeExecutorsJson(nes);
        JsonNode root = MAPPER.readTree(json);

        // Verify native_executors and task_monitors wrappers are absent
        assertFalse(root.has("native_executors"), "native_executors wrapper must be absent");
        assertFalse(root.has("task_monitors"), "task_monitors wrapper must be absent");

        // Verify io_runtime is a top-level key with all 9 fields
        JsonNode ioRuntime = root.get("io_runtime");
        assertNotNull(ioRuntime, "io_runtime must be present at top level");
        verifyRuntimeFields(nes.getIoRuntime(), ioRuntime);

        // Verify cpu_runtime is a top-level key with all 9 fields (present case)
        JsonNode cpuRuntime = root.get("cpu_runtime");
        assertNotNull(cpuRuntime, "cpu_runtime must be present at top level when non-null");
        verifyRuntimeFields(nes.getCpuRuntime(), cpuRuntime);

        // Verify each task monitor is a top-level key with correct fields
        for (OperationType opType : OperationType.values()) {
            JsonNode monitor = root.get(opType.key());
            assertNotNull(monitor, opType.key() + " must be present at top level");
            verifyTaskMonitorFields(nes.getTaskMonitors().get(opType.key()), monitor, opType.key());
        }
    }

    /**
     * Feature: stats-endpoint-refactor, Property 1 (CPU absent variant).
     *
     * <p>Validates: Requirements 2.1, 2.4, 2.5, 3.1, 3.2
     */
    @Property(tries = 200)
    void flatJsonSerializationPreservesAllMetricValuesCpuAbsent(@ForAll("nativeExecutorsStatsCpuAbsent") NativeExecutorsStats nes)
        throws IOException {
        String json = renderNativeExecutorsJson(nes);
        JsonNode root = MAPPER.readTree(json);

        // Verify native_executors and task_monitors wrappers are absent
        assertFalse(root.has("native_executors"), "native_executors wrapper must be absent");
        assertFalse(root.has("task_monitors"), "task_monitors wrapper must be absent");

        // Verify io_runtime is a top-level key with all 9 fields
        JsonNode ioRuntime = root.get("io_runtime");
        assertNotNull(ioRuntime, "io_runtime must be present at top level");
        verifyRuntimeFields(nes.getIoRuntime(), ioRuntime);

        // cpu_runtime absent
        assertFalse(root.has("cpu_runtime"), "cpu_runtime must be absent when null");

        // Verify each task monitor is a top-level key with correct fields
        for (OperationType opType : OperationType.values()) {
            JsonNode monitor = root.get(opType.key());
            assertNotNull(monitor, opType.key() + " must be present at top level");
            verifyTaskMonitorFields(nes.getTaskMonitors().get(opType.key()), monitor, opType.key());
        }
    }

    // ---- Property 2: CPU runtime conditional presence ----

    /**
     * Feature: stats-endpoint-refactor, Property 2: CPU runtime conditional presence (present case).
     *
     * <p>For any valid NativeExecutorsStats with non-null cpuRuntime, serialized JSON contains
     * cpu_runtime top-level key with correct values.
     *
     * <p>Validates: Requirements 2.2, 2.3
     */
    @Property(tries = 200)
    void cpuRuntimePresentWhenNonNull(@ForAll("nativeExecutorsStatsCpuPresent") NativeExecutorsStats nes) throws IOException {
        String json = renderNativeExecutorsJson(nes);
        JsonNode root = MAPPER.readTree(json);

        assertTrue(root.has("cpu_runtime"), "cpu_runtime must be present when cpuRuntime is non-null");
        JsonNode cpuRuntime = root.get("cpu_runtime");
        verifyRuntimeFields(nes.getCpuRuntime(), cpuRuntime);
    }

    /**
     * Feature: stats-endpoint-refactor, Property 2: CPU runtime conditional presence (absent case).
     *
     * <p>For any valid NativeExecutorsStats with null cpuRuntime, serialized JSON does not contain
     * cpu_runtime key.
     *
     * <p>Validates: Requirements 2.2, 2.3
     */
    @Property(tries = 200)
    void cpuRuntimeAbsentWhenNull(@ForAll("nativeExecutorsStatsCpuAbsent") NativeExecutorsStats nes) throws IOException {
        String json = renderNativeExecutorsJson(nes);
        JsonNode root = MAPPER.readTree(json);

        assertFalse(root.has("cpu_runtime"), "cpu_runtime must be absent when cpuRuntime is null");
    }

    // ---- Property 3: Transport serialization round-trip ----

    /**
     * Feature: stats-endpoint-refactor, Property 3: Transport serialization round-trip.
     *
     * <p>For any valid DataFusionStats, writing to StreamOutput and reading back from StreamInput
     * produces an object equal to the original.
     *
     * <p>Validates: Requirements 4.1, 4.2
     */
    @Property(tries = 200)
    void transportSerializationRoundTrip(@ForAll("dataFusionStats") DataFusionStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        DataFusionStats deserialized = new DataFusionStats(in);
        assertEquals(original, deserialized, "Transport round-trip must preserve all fields");
    }

    // ---- Helper methods ----

    private String renderNativeExecutorsJson(NativeExecutorsStats nes) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        nes.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    private void verifyRuntimeFields(RuntimeMetrics rm, JsonNode runtimeNode) {
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
            assertTrue(runtimeNode.has(fieldName), "Runtime field '" + fieldName + "' must be present");
            assertEquals(expected[i], runtimeNode.get(fieldName).asLong(), "Runtime field '" + fieldName + "': expected " + expected[i]);
        }
    }

    private void verifyTaskMonitorFields(TaskMonitorStats tm, JsonNode monitorNode, String opType) {
        long[] expected = { tm.totalPollDurationMs, tm.totalScheduledDurationMs, tm.totalIdleDurationMs };
        for (int i = 0; i < TASK_FIELD_NAMES.length; i++) {
            String fieldName = TASK_FIELD_NAMES[i];
            assertTrue(monitorNode.has(fieldName), opType + " field '" + fieldName + "' must be present");
            assertEquals(expected[i], monitorNode.get(fieldName).asLong(), opType + " field '" + fieldName + "': expected " + expected[i]);
        }
    }
}
