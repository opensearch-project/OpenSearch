/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.plugin.stats.NativeExecutorsStats;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Unit tests for {@link StatsLayout} — verifies layout size, VarHandle reads,
 * and cpu_runtime null/non-null logic.
 */
public class StatsLayoutTests extends OpenSearchTestCase {

    /** 7.1: Layout byte size must be 224 (28 × 8). */
    public void testLayoutByteSize() {
        assertEquals(224L, StatsLayout.LAYOUT.byteSize());
        assertEquals(28 * Long.BYTES, (int) StatsLayout.LAYOUT.byteSize());
    }

    /** 7.2: readRuntimeMetrics decodes 8 known values from io_runtime group. */
    public void testReadRuntimeMetricsFromSegment() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            // Write sequential values 1-8 at io_runtime positions (indices 0-7)
            for (int i = 0; i < 8; i++) {
                seg.setAtIndex(ValueLayout.JAVA_LONG, i, i + 1L);
            }

            var rt = StatsLayout.readRuntimeMetrics(seg, "io_runtime");
            assertEquals(1L, rt.workersCount);
            assertEquals(2L, rt.totalPollsCount);
            assertEquals(3L, rt.totalBusyDurationMs);
            assertEquals(4L, rt.totalOverflowCount);
            assertEquals(5L, rt.globalQueueDepth);
            assertEquals(6L, rt.blockingQueueDepth);
            assertEquals(7L, rt.numAliveTasks);
            assertEquals(8L, rt.spawnedTasksCount);
        }
    }

    /** 7.3: readTaskMonitor decodes 3 known values from query_execution group. */
    public void testReadTaskMonitorFromSegment() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            // query_execution starts at index 16
            seg.setAtIndex(ValueLayout.JAVA_LONG, 16, 100L);
            seg.setAtIndex(ValueLayout.JAVA_LONG, 17, 200L);
            seg.setAtIndex(ValueLayout.JAVA_LONG, 18, 300L);

            var tm = StatsLayout.readTaskMonitor(seg, "query_execution");
            assertEquals(100L, tm.totalPollDurationMs);
            assertEquals(200L, tm.totalScheduledDurationMs);
            assertEquals(300L, tm.totalIdleDurationMs);
        }
    }

    /** 7.4: cpu_runtime is null when workers_count == 0. */
    public void testCpuRuntimeNullWhenWorkersZero() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            // cpu_runtime.workers_count is at index 8 — leave it as 0 (default)
            long cpuWorkers = StatsLayout.readField(seg, "cpu_runtime", "workers_count");
            assertEquals(0L, cpuWorkers);

            // Simulate the NativeBridge logic
            NativeExecutorsStats.RuntimeMetrics cpuRuntime = null;
            if (cpuWorkers > 0) {
                cpuRuntime = StatsLayout.readRuntimeMetrics(seg, "cpu_runtime");
            }
            assertNull(cpuRuntime);
        }
    }

    /** 7.5: cpu_runtime is non-null when workers_count > 0. */
    public void testCpuRuntimeNonNullWhenWorkersPositive() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            // Set cpu_runtime.workers_count (index 8) to 5
            seg.setAtIndex(ValueLayout.JAVA_LONG, 8, 5L);
            // Set other cpu_runtime fields (indices 9-15)
            for (int i = 9; i <= 15; i++) {
                seg.setAtIndex(ValueLayout.JAVA_LONG, i, i * 10L);
            }

            long cpuWorkers = StatsLayout.readField(seg, "cpu_runtime", "workers_count");
            assertEquals(5L, cpuWorkers);

            NativeExecutorsStats.RuntimeMetrics cpuRuntime = null;
            if (cpuWorkers > 0) {
                cpuRuntime = StatsLayout.readRuntimeMetrics(seg, "cpu_runtime");
            }
            assertNotNull(cpuRuntime);
            assertEquals(5L, cpuRuntime.workersCount);
            assertEquals(90L, cpuRuntime.totalPollsCount);
        }
    }
}
