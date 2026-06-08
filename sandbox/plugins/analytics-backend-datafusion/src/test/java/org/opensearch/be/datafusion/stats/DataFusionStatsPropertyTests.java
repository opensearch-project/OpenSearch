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
 * Randomized tests for {@link DataFusionStats} constructed via direct constructors.
 *
 * <p>Tests construct objects directly — no decode path, no ArrayCursor.
 *
 * <p>Feature: ffm-stats-decode
 */
public class DataFusionStatsPropertyTests extends OpenSearchTestCase {

    private static final int TRIES = 200;
    private static final int TRIES_NULL = 100;

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

    private DataFusionStats randomDataFusionStatsCpuPresent() {
        return new DataFusionStats(
            new NativeExecutorsStats(randomRuntimeMetrics(), randomRuntimeMetricsWithPositiveWorkers(), randomTaskMonitors()),
            new PartitionGateStats("datanode_gate", 12, 0, 0, 0),
            new PartitionGateStats("coordinator_gate", 12, 0, 0, 0)
        );
    }

    private DataFusionStats randomDataFusionStatsCpuAbsent() {
        return new DataFusionStats(
            new NativeExecutorsStats(randomRuntimeMetrics(), null, randomTaskMonitors()),
            new PartitionGateStats("datanode_gate", 12, 0, 0, 0),
            new PartitionGateStats("coordinator_gate", 12, 0, 0, 0)
        );
    }

    private DataFusionStats dataFusionStatsNullExecutors() {
        return new DataFusionStats(null, null, null);
    }

    // ---- Property 1: Writeable round-trip preserves all field values ----

    public void testWriteableRoundTripCpuPresent() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStats original = randomDataFusionStatsCpuPresent();
            DataFusionStats deserialized = writeableRoundTrip(original);
            assertEquals("Writeable round-trip must preserve all fields (CPU present)", original, deserialized);
        }
    }

    public void testWriteableRoundTripCpuAbsent() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStats original = randomDataFusionStatsCpuAbsent();
            DataFusionStats deserialized = writeableRoundTrip(original);
            assertEquals("Writeable round-trip must preserve all fields (CPU absent)", original, deserialized);
        }
    }

    public void testWriteableRoundTripNullExecutors() throws IOException {
        for (int i = 0; i < TRIES_NULL; i++) {
            DataFusionStats original = dataFusionStatsNullExecutors();
            DataFusionStats deserialized = writeableRoundTrip(original);
            assertEquals("Writeable round-trip must preserve null executors", original, deserialized);
        }
    }

    // ---- Property 2: toXContent round-trip preserves all field values ----

    public void testToXContentRoundTripCpuPresent() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStats stats = randomDataFusionStatsCpuPresent();
            NativeExecutorsStats nes = stats.getNativeExecutorsStats();
            assertNotNull(nes);

            Map<String, Object> root = renderToMap(stats);

            // IO runtime: 9 fields
            Map<String, Object> ioRuntime = asMap(root.get("io_runtime"));
            assertNotNull("io_runtime must be present", ioRuntime);
            assertEquals("io_runtime must have exactly 9 fields", 9, ioRuntime.size());
            verifyRuntimeFields(nes.getIoRuntime(), ioRuntime);

            // CPU runtime: 9 fields
            assertTrue("cpu_runtime must be present", root.containsKey("cpu_runtime"));
            Map<String, Object> cpuRuntime = asMap(root.get("cpu_runtime"));
            assertEquals("cpu_runtime must have exactly 9 fields", 9, cpuRuntime.size());
            verifyRuntimeFields(nes.getCpuRuntime(), cpuRuntime);

            // Task monitors: 4 ops x 3 fields (at top level, no task_monitors wrapper)
            for (OperationType opType : OperationType.values()) {
                Map<String, Object> monitor = asMap(root.get(opType.key()));
                assertNotNull(opType.key() + " must be present", monitor);
                assertEquals(3, monitor.size());
                verifyTaskMonitorFields(nes.getTaskMonitors().get(opType.key()), monitor, opType.key());
            }
        }
    }

    public void testToXContentRoundTripCpuAbsent() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStats stats = randomDataFusionStatsCpuAbsent();
            NativeExecutorsStats nes = stats.getNativeExecutorsStats();
            assertNotNull(nes);

            Map<String, Object> root = renderToMap(stats);

            // IO runtime: 9 fields
            Map<String, Object> ioRuntime = asMap(root.get("io_runtime"));
            assertNotNull("io_runtime must be present", ioRuntime);
            assertEquals("io_runtime must have exactly 9 fields", 9, ioRuntime.size());
            verifyRuntimeFields(nes.getIoRuntime(), ioRuntime);

            // CPU runtime absent
            assertFalse("cpu_runtime must be absent when cpuRuntime is null", root.containsKey("cpu_runtime"));

            // Task monitors: at top level, no task_monitors wrapper
            for (OperationType opType : OperationType.values()) {
                Map<String, Object> monitor = asMap(root.get(opType.key()));
                assertNotNull(opType.key() + " must be present", monitor);
                assertEquals(3, monitor.size());
                verifyTaskMonitorFields(nes.getTaskMonitors().get(opType.key()), monitor, opType.key());
            }
        }
    }

    // ---- Property 3: toXContent determinism ----

    public void testToXContentDeterminismCpuPresent() throws IOException {
        for (int i = 0; i < TRIES_NULL; i++) {
            DataFusionStats stats = randomDataFusionStatsCpuPresent();
            byte[] first = renderJsonBytes(stats);
            byte[] second = renderJsonBytes(stats);
            assertArrayEquals("toXContent must produce byte-for-byte identical JSON on repeated calls (CPU present)", first, second);
        }
    }

    public void testToXContentDeterminismCpuAbsent() throws IOException {
        for (int i = 0; i < TRIES_NULL; i++) {
            DataFusionStats stats = randomDataFusionStatsCpuAbsent();
            byte[] first = renderJsonBytes(stats);
            byte[] second = renderJsonBytes(stats);
            assertArrayEquals("toXContent must produce byte-for-byte identical JSON on repeated calls (CPU absent)", first, second);
        }
    }

    public void testToXContentDeterminismNullExecutors() throws IOException {
        for (int i = 0; i < TRIES_NULL; i++) {
            DataFusionStats stats = dataFusionStatsNullExecutors();
            byte[] first = renderJsonBytes(stats);
            byte[] second = renderJsonBytes(stats);
            assertArrayEquals("toXContent must produce byte-for-byte identical JSON on repeated calls (null executors)", first, second);
        }
    }

    // ---- Helper methods ----

    private byte[] renderJsonBytes(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return BytesReference.toBytes(BytesReference.bytes(builder));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> renderToMap(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    private DataFusionStats writeableRoundTrip(DataFusionStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new DataFusionStats(in);
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
