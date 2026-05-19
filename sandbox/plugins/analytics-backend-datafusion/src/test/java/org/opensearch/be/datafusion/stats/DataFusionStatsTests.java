/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unit tests for {@link DataFusionStats} constructed via direct constructors.
 *
 * <p>Layout: IO RuntimeMetrics (9 fields), optional CPU RuntimeMetrics (9 fields),
 * 4 TaskMonitorStats (3 fields each).
 */
public class DataFusionStatsTests extends OpenSearchTestCase {

    /** Build a DataFusionStats with sequential values for deterministic field verification. */
    private static DataFusionStats sequentialStats() {
        RuntimeMetrics io = new RuntimeMetrics(1, 2, 3, 4, 5, 6, 7, 8, 0);
        RuntimeMetrics cpu = new RuntimeMetrics(9, 10, 11, 12, 13, 14, 15, 16, 0);
        Map<String, TaskMonitorStats> taskMonitors = new LinkedHashMap<>();
        taskMonitors.put("coordinator_reduce", new TaskMonitorStats(17, 18, 19));
        taskMonitors.put("query_execution", new TaskMonitorStats(20, 21, 22));
        taskMonitors.put("stream_next", new TaskMonitorStats(23, 24, 25));
        taskMonitors.put("plan_setup", new TaskMonitorStats(26, 27, 28));
        return new DataFusionStats(new NativeExecutorsStats(io, cpu, taskMonitors));
    }

    private static String toJsonString(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    // ---- Test: sequential construction verifies each field ----

    public void testSequentialConstructionVerifiesFields() {
        DataFusionStats stats = sequentialStats();
        NativeExecutorsStats nes = stats.getNativeExecutorsStats();
        assertNotNull(nes);

        // IO runtime (values 1-8)
        RuntimeMetrics io = nes.getIoRuntime();
        assertNotNull(io);
        assertEquals(1L, io.workersCount);
        assertEquals(2L, io.totalPollsCount);
        assertEquals(3L, io.totalBusyDurationMs);
        assertEquals(4L, io.totalOverflowCount);
        assertEquals(5L, io.globalQueueDepth);
        assertEquals(6L, io.blockingQueueDepth);
        assertEquals(7L, io.numAliveTasks);
        assertEquals(8L, io.spawnedTasksCount);

        // CPU runtime (values 9-16)
        RuntimeMetrics cpu = nes.getCpuRuntime();
        assertNotNull(cpu);
        assertEquals(9L, cpu.workersCount);
        assertEquals(10L, cpu.totalPollsCount);
        assertEquals(11L, cpu.totalBusyDurationMs);
        assertEquals(12L, cpu.totalOverflowCount);
        assertEquals(13L, cpu.globalQueueDepth);
        assertEquals(14L, cpu.blockingQueueDepth);
        assertEquals(15L, cpu.numAliveTasks);
        assertEquals(16L, cpu.spawnedTasksCount);

        // Task monitors
        Map<String, TaskMonitorStats> monitors = nes.getTaskMonitors();
        assertEquals(4, monitors.size());

        TaskMonitorStats cr = monitors.get("coordinator_reduce");
        assertNotNull(cr);
        assertEquals(17L, cr.totalPollDurationMs);
        assertEquals(18L, cr.totalScheduledDurationMs);
        assertEquals(19L, cr.totalIdleDurationMs);

        TaskMonitorStats qe = monitors.get("query_execution");
        assertNotNull(qe);
        assertEquals(20L, qe.totalPollDurationMs);
        assertEquals(21L, qe.totalScheduledDurationMs);
        assertEquals(22L, qe.totalIdleDurationMs);

        TaskMonitorStats sn = monitors.get("stream_next");
        assertNotNull(sn);
        assertEquals(23L, sn.totalPollDurationMs);
        assertEquals(24L, sn.totalScheduledDurationMs);
        assertEquals(25L, sn.totalIdleDurationMs);

        TaskMonitorStats ps = monitors.get("plan_setup");
        assertNotNull(ps);
        assertEquals(26L, ps.totalPollDurationMs);
        assertEquals(27L, ps.totalScheduledDurationMs);
        assertEquals(28L, ps.totalIdleDurationMs);
    }

    // ---- Test: CPU runtime null → cpuRuntime absent in JSON ----

    public void testCpuRuntimeAbsentWhenNull() throws IOException {
        RuntimeMetrics io = new RuntimeMetrics(100, 101, 102, 103, 104, 105, 106, 107, 0);
        Map<String, TaskMonitorStats> taskMonitors = new LinkedHashMap<>();
        taskMonitors.put("coordinator_reduce", new TaskMonitorStats(14, 15, 16));
        taskMonitors.put("query_execution", new TaskMonitorStats(17, 18, 19));
        taskMonitors.put("stream_next", new TaskMonitorStats(20, 21, 22));
        taskMonitors.put("plan_setup", new TaskMonitorStats(23, 24, 25));

        DataFusionStats stats = new DataFusionStats(new NativeExecutorsStats(io, null, taskMonitors));
        assertNull(stats.getNativeExecutorsStats().getCpuRuntime());

        String json = toJsonString(stats);
        assertFalse("cpu_runtime should be omitted when null", json.contains("cpu_runtime"));
        assertTrue("io_runtime should still be present", json.contains("io_runtime"));
        // Task monitors are at top level (flat structure, no "task_monitors" wrapper)
        assertTrue("coordinator_reduce should still be present", json.contains("coordinator_reduce"));
        assertTrue("query_execution should still be present", json.contains("query_execution"));
        assertTrue("stream_next should still be present", json.contains("stream_next"));
        assertTrue("plan_setup should still be present", json.contains("plan_setup"));
    }

    // ---- Test: non-null CPU runtime → cpuRuntime present in JSON ----

    public void testCpuRuntimePresentWhenNonNull() throws IOException {
        DataFusionStats stats = sequentialStats();
        assertNotNull(stats.getNativeExecutorsStats().getCpuRuntime());

        String json = toJsonString(stats);
        assertTrue("cpu_runtime should be present", json.contains("cpu_runtime"));

        String[] runtimeFieldNames = {
            "workers_count",
            "total_polls_count",
            "total_busy_duration_ms",
            "total_overflow_count",
            "global_queue_depth",
            "blocking_queue_depth",
            "num_alive_tasks",
            "spawned_tasks_count" };
        for (String field : runtimeFieldNames) {
            assertTrue("JSON should contain field: " + field, json.contains("\"" + field + "\""));
        }
    }

    // ---- Test: toXContent renders correct JSON structure ----

    public void testToXContentJsonStructure() throws IOException {
        DataFusionStats stats = sequentialStats();
        String json = toJsonString(stats);

        // Flat structure: no "native_executors" or "task_monitors" wrappers
        assertFalse(json.contains("\"native_executors\""));
        assertTrue(json.contains("\"io_runtime\""));
        assertTrue(json.contains("\"cpu_runtime\""));
        assertFalse(json.contains("\"task_monitors\""));

        // Task monitors at top level
        assertTrue(json.contains("\"coordinator_reduce\""));
        assertTrue(json.contains("\"query_execution\""));
        assertTrue(json.contains("\"stream_next\""));
        assertTrue(json.contains("\"plan_setup\""));

        String[] taskFields = { "total_poll_duration_ms", "total_scheduled_duration_ms", "total_idle_duration_ms" };
        for (String field : taskFields) {
            assertTrue("JSON should contain task monitor field: " + field, json.contains("\"" + field + "\""));
        }

        // IO runtime: workers_count = 1
        assertTrue(json.contains("\"workers_count\":1"));
        // coordinator_reduce: total_poll_duration_ms = 17
        assertTrue(json.contains("\"total_poll_duration_ms\":17"));
    }

    // ---- Test: toXContent with CPU runtime omitted ----

    public void testToXContentCpuRuntimeOmitted() throws IOException {
        RuntimeMetrics io = new RuntimeMetrics(100, 101, 102, 103, 104, 105, 106, 107, 0);
        Map<String, TaskMonitorStats> taskMonitors = new LinkedHashMap<>();
        taskMonitors.put("coordinator_reduce", new TaskMonitorStats(14, 15, 16));
        taskMonitors.put("query_execution", new TaskMonitorStats(17, 18, 19));
        taskMonitors.put("stream_next", new TaskMonitorStats(20, 21, 22));
        taskMonitors.put("plan_setup", new TaskMonitorStats(23, 24, 25));

        DataFusionStats stats = new DataFusionStats(new NativeExecutorsStats(io, null, taskMonitors));
        String json = toJsonString(stats);

        assertTrue(json.contains("\"io_runtime\""));
        assertFalse("cpu_runtime should not appear", json.contains("\"cpu_runtime\""));
        // Task monitors at top level (no wrapper)
        assertTrue(json.contains("\"coordinator_reduce\""));
        assertTrue(json.contains("\"query_execution\""));
        assertTrue(json.contains("\"plan_setup\""));
    }

    // ---- Test: exactly 4 task monitor keys ----

    public void testExactlyFourTaskMonitors() {
        DataFusionStats stats = sequentialStats();
        Map<String, TaskMonitorStats> monitors = stats.getNativeExecutorsStats().getTaskMonitors();

        assertEquals(4, monitors.size());
        assertTrue(monitors.containsKey("coordinator_reduce"));
        assertTrue(monitors.containsKey("query_execution"));
        assertTrue(monitors.containsKey("stream_next"));
        assertTrue(monitors.containsKey("plan_setup"));
    }
}
