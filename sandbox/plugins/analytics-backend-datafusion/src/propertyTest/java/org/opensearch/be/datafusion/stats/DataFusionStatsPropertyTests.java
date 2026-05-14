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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
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
 * Property-based tests for {@link DataFusionStats} constructed via direct constructors.
 *
 * <p>Tests construct objects directly — no decode path, no ArrayCursor.
 *
 * <p>Tag: Feature: ffm-stats-decode
 */
public class DataFusionStatsPropertyTests {

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

    /** DataFusionStats with CPU runtime present (workersCount > 0). */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsCpuPresent() {
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
        }), taskMonitorStats(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats()).as((io, cpu, qe, sn, fp, ss) -> {
            Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
            monitors.put("query_execution", qe);
            monitors.put("stream_next", sn);
            monitors.put("fetch_phase", fp);
            monitors.put("segment_stats", ss);
            return new DataFusionStats(new NativeExecutorsStats(io, cpu, monitors));
        });
    }

    /** DataFusionStats with CPU runtime absent (null). */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsCpuAbsent() {
        return Combinators.combine(runtimeMetrics(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats())
            .as((io, qe, sn, fp, ss) -> {
                Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
                monitors.put("query_execution", qe);
                monitors.put("stream_next", sn);
                monitors.put("fetch_phase", fp);
                monitors.put("segment_stats", ss);
                return new DataFusionStats(new NativeExecutorsStats(io, null, monitors));
            });
    }

    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsNullExecutors() {
        return Arbitraries.just(new DataFusionStats((NativeExecutorsStats) null));
    }

    // ---- Property 1: Writeable round-trip preserves all field values ----

    /**
     * Feature: stats-spi-refactor, Property 1: DataFusionStats Writeable round-trip (CPU present).
     *
     * <p>Validates: Requirements 5.6
     */
    @Property(tries = 200)
    void writeableRoundTripCpuPresent(@ForAll("dataFusionStatsCpuPresent") DataFusionStats original) throws IOException {
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Writeable round-trip must preserve all fields (CPU present)");
    }

    /**
     * Feature: stats-spi-refactor, Property 1: DataFusionStats Writeable round-trip (CPU absent).
     *
     * <p>Validates: Requirements 5.6
     */
    @Property(tries = 200)
    void writeableRoundTripCpuAbsent(@ForAll("dataFusionStatsCpuAbsent") DataFusionStats original) throws IOException {
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Writeable round-trip must preserve all fields (CPU absent)");
    }

    /**
     * Feature: stats-spi-refactor, Property 1: DataFusionStats Writeable round-trip (null executors).
     *
     * <p>Validates: Requirements 5.6
     */
    @Property(tries = 100)
    void writeableRoundTripNullExecutors(@ForAll("dataFusionStatsNullExecutors") DataFusionStats original) throws IOException {
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Writeable round-trip must preserve null executors");
    }

    // ---- Property 2: toXContent round-trip preserves all field values ----

    /**
     * Feature: ffm-stats-decode, Property 2: toXContent round-trip (CPU present).
     */
    @Property(tries = 200)
    void toXContentRoundTripCpuPresent(@ForAll("dataFusionStatsCpuPresent") DataFusionStats stats) throws IOException {
        NativeExecutorsStats nes = stats.getNativeExecutorsStats();
        assertNotNull(nes);

        String json = renderJson(stats);
        JsonNode root = MAPPER.readTree(json);

        // IO runtime: 9 fields
        JsonNode ioRuntime = root.get("io_runtime");
        assertNotNull(ioRuntime, "io_runtime must be present");
        assertEquals(9, ioRuntime.size(), "io_runtime must have exactly 9 fields");
        verifyRuntimeFields(nes.getIoRuntime(), ioRuntime);

        // CPU runtime: 9 fields
        assertTrue(root.has("cpu_runtime"), "cpu_runtime must be present");
        JsonNode cpuRuntime = root.get("cpu_runtime");
        assertEquals(9, cpuRuntime.size(), "cpu_runtime must have exactly 9 fields");
        verifyRuntimeFields(nes.getCpuRuntime(), cpuRuntime);

        // Task monitors: 4 ops × 3 fields (at top level, no task_monitors wrapper)
        for (OperationType opType : OperationType.values()) {
            JsonNode monitor = root.get(opType.key());
            assertNotNull(monitor, opType.key() + " must be present");
            assertEquals(3, monitor.size());
            verifyTaskMonitorFields(nes.getTaskMonitors().get(opType.key()), monitor, opType.key());
        }
    }

    /**
     * Feature: ffm-stats-decode, Property 2: toXContent round-trip (CPU absent).
     */
    @Property(tries = 200)
    void toXContentRoundTripCpuAbsent(@ForAll("dataFusionStatsCpuAbsent") DataFusionStats stats) throws IOException {
        NativeExecutorsStats nes = stats.getNativeExecutorsStats();
        assertNotNull(nes);

        String json = renderJson(stats);
        JsonNode root = MAPPER.readTree(json);

        // IO runtime: 9 fields
        JsonNode ioRuntime = root.get("io_runtime");
        assertNotNull(ioRuntime, "io_runtime must be present");
        assertEquals(9, ioRuntime.size(), "io_runtime must have exactly 9 fields");
        verifyRuntimeFields(nes.getIoRuntime(), ioRuntime);

        // CPU runtime absent
        assertFalse(root.has("cpu_runtime"), "cpu_runtime must be absent when cpuRuntime is null");

        // Task monitors: at top level, no task_monitors wrapper
        for (OperationType opType : OperationType.values()) {
            JsonNode monitor = root.get(opType.key());
            assertNotNull(monitor, opType.key() + " must be present");
            assertEquals(3, monitor.size());
            verifyTaskMonitorFields(nes.getTaskMonitors().get(opType.key()), monitor, opType.key());
        }
    }

    // ---- Property 3: toXContent determinism (merged from SPI module) ----

    /**
     * Feature: stats-spi-refactor, Property: DataFusionStats toXContent determinism (CPU present).
     *
     * <p>Validates: Requirements 10.3
     */
    @Property(tries = 100)
    void toXContentDeterminismCpuPresent(@ForAll("dataFusionStatsCpuPresent") DataFusionStats stats) throws IOException {
        byte[] first = renderJsonBytes(stats);
        byte[] second = renderJsonBytes(stats);
        assertTrue(Arrays.equals(first, second), "toXContent must produce byte-for-byte identical JSON on repeated calls (CPU present)");
    }

    /**
     * Feature: stats-spi-refactor, Property: DataFusionStats toXContent determinism (CPU absent).
     *
     * <p>Validates: Requirements 10.3
     */
    @Property(tries = 100)
    void toXContentDeterminismCpuAbsent(@ForAll("dataFusionStatsCpuAbsent") DataFusionStats stats) throws IOException {
        byte[] first = renderJsonBytes(stats);
        byte[] second = renderJsonBytes(stats);
        assertTrue(Arrays.equals(first, second), "toXContent must produce byte-for-byte identical JSON on repeated calls (CPU absent)");
    }

    /**
     * Feature: stats-spi-refactor, Property: DataFusionStats toXContent determinism (null executors).
     *
     * <p>Validates: Requirements 10.3
     */
    @Property(tries = 100)
    void toXContentDeterminismNullExecutors(@ForAll("dataFusionStatsNullExecutors") DataFusionStats stats) throws IOException {
        byte[] first = renderJsonBytes(stats);
        byte[] second = renderJsonBytes(stats);
        assertTrue(Arrays.equals(first, second), "toXContent must produce byte-for-byte identical JSON on repeated calls (null executors)");
    }

    /** Renders a {@link DataFusionStats} to JSON bytes via {@code toXContent}. */
    private byte[] renderJsonBytes(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return BytesReference.toBytes(BytesReference.bytes(builder));
    }

    // ---- Helper methods ----

    private String renderJson(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    private DataFusionStats writeableRoundTrip(DataFusionStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new DataFusionStats(in);
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
