/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.be.datafusion.stats.AdaptiveBudgetStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;

/**
 * Unit tests for {@link StatsLayout} — verifies layout size, VarHandle reads,
 * and cpu_runtime null/non-null logic.
 */
public class StatsLayoutTests extends OpenSearchTestCase {

    /** 7.1: Layout byte size must be 640 (85 × 8). */
    public void testLayoutByteSize() {
        assertEquals(680L, StatsLayout.LAYOUT.byteSize());
        assertEquals(85 * Long.BYTES, (int) StatsLayout.LAYOUT.byteSize());
    }

    /** 7.2: readRuntimeMetrics decodes 9 known values from io_runtime group. */
    public void testReadRuntimeMetricsFromSegment() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            // Write sequential values 1-9 at io_runtime positions (indices 0-8)
            for (int i = 0; i < 9; i++) {
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
            assertEquals(9L, rt.totalLocalQueueDepth);
        }
    }

    /** 7.3: readTaskMonitor decodes 5 known values from coordinator_reduce group. */
    public void testReadTaskMonitorFromSegment() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            // coordinator_reduce starts at index 18 (2 runtime groups × 9 fields = 18)
            seg.setAtIndex(ValueLayout.JAVA_LONG, 18, 100L);
            seg.setAtIndex(ValueLayout.JAVA_LONG, 19, 200L);
            seg.setAtIndex(ValueLayout.JAVA_LONG, 20, 300L);
            seg.setAtIndex(ValueLayout.JAVA_LONG, 21, 400L);
            seg.setAtIndex(ValueLayout.JAVA_LONG, 22, 395L);

            var tm = StatsLayout.readTaskMonitor(seg, "coordinator_reduce");
            assertEquals(100L, tm.totalPollDurationMs);
            assertEquals(200L, tm.totalScheduledDurationMs);
            assertEquals(300L, tm.totalIdleDurationMs);
            assertEquals(400L, tm.instrumentedCount);
            assertEquals(395L, tm.droppedCount);
        }
    }

    /** 7.4: cpu_runtime is null when workers_count == 0. */
    public void testCpuRuntimeNullWhenWorkersZero() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            // cpu_runtime.workers_count is at index 9 — leave it as 0 (default)
            long cpuWorkers = StatsLayout.readField(seg, "cpu_runtime", "workers_count");
            assertEquals(0L, cpuWorkers);

            // Simulate the NativeBridge logic
            RuntimeMetrics cpuRuntime = null;
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
            // Set cpu_runtime.workers_count (index 9) to 5
            seg.setAtIndex(ValueLayout.JAVA_LONG, 9, 5L);
            // Set other cpu_runtime fields (indices 10-17)
            for (int i = 10; i <= 17; i++) {
                seg.setAtIndex(ValueLayout.JAVA_LONG, i, i * 10L);
            }

            long cpuWorkers = StatsLayout.readField(seg, "cpu_runtime", "workers_count");
            assertEquals(5L, cpuWorkers);

            RuntimeMetrics cpuRuntime = null;
            if (cpuWorkers > 0) {
                cpuRuntime = StatsLayout.readRuntimeMetrics(seg, "cpu_runtime");
            }
            assertNotNull(cpuRuntime);
            assertEquals(5L, cpuRuntime.workersCount);
            assertEquals(100L, cpuRuntime.totalPollsCount);
        }
    }

    /** 7.6: readPartitionGate decodes 8 fields including pendingAcquirePermits and pendingAcquireBatches from fragment_executor_gate. */
    public void testReadPartitionGateFromSegment() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            // fragment_executor_gate starts at index:
            // 2 runtimes × 9 = 18, + 4 task monitors × 5 = 20 → starts at index 38
            int gateStart = 18 + 20; // = 38
            seg.setAtIndex(ValueLayout.JAVA_LONG, gateStart, 12L);     // max_permits
            seg.setAtIndex(ValueLayout.JAVA_LONG, gateStart + 1, 3L);  // active_permits
            seg.setAtIndex(ValueLayout.JAVA_LONG, gateStart + 2, 456L);// total_wait_duration_ms
            seg.setAtIndex(ValueLayout.JAVA_LONG, gateStart + 3, 100L);// total_batches_started
            seg.setAtIndex(ValueLayout.JAVA_LONG, gateStart + 4, 1L);  // poison_permits
            seg.setAtIndex(ValueLayout.JAVA_LONG, gateStart + 5, 12L); // target_max_permits
            seg.setAtIndex(ValueLayout.JAVA_LONG, gateStart + 6, 5L);  // pending_acquire_permits
            seg.setAtIndex(ValueLayout.JAVA_LONG, gateStart + 7, 2L);  // pending_acquire_batches

            PartitionGateStats gate = StatsLayout.readPartitionGate(seg, "fragment_executor_gate");
            assertEquals(12L, gate.maxPermits);
            assertEquals(3L, gate.activePermits);
            assertEquals(456L, gate.totalWaitDurationMs);
            assertEquals(100L, gate.totalBatchesStarted);
            assertEquals(1L, gate.poisonPermits);
            assertEquals(12L, gate.targetMaxPermits);
            assertEquals(5L, gate.pendingAcquirePermits);
            assertEquals(2L, gate.pendingAcquireBatches);
        }
    }

    /** 7.7: readAdaptiveBudgetStats decodes 2 fields from adaptive_budget group. */
    public void testReadAdaptiveBudgetStatsFromSegment() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(StatsLayout.LAYOUT);
            // adaptive_budget starts at index:
            // 2 runtimes × 9 = 18, + 4 task monitors × 5 = 20, + 1 gate × 8 = 8 → starts at index 46
            int budgetStart = 18 + 20 + 8; // = 46
            seg.setAtIndex(ValueLayout.JAVA_LONG, budgetStart, 15L);       // fallbacks
            seg.setAtIndex(ValueLayout.JAVA_LONG, budgetStart + 1, 2L);    // rejections

            AdaptiveBudgetStats bs = StatsLayout.readAdaptiveBudgetStats(seg);
            assertEquals(15L, bs.fallbacks);
            assertEquals(2L, bs.rejections);
        }
    }
}
