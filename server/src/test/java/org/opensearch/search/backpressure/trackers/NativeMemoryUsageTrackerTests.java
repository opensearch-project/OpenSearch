/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

/**
 * Unit tests for {@link NativeMemoryUsageTracker}.
 *
 * <p>The tracker maintains static suppliers (snapshot + budget) that survive across tests
 * because they're installed by backend plugins at boot time. {@link #setUp()} and
 * {@link #tearDown()} reset them to their defaults so every test starts from a known
 * "no backend installed" state.
 */
public class NativeMemoryUsageTrackerTests extends OpenSearchTestCase {

    @Before
    public void resetStaticsBefore() {
        // Static suppliers are package-shared with the production wiring; clear before every test
        // so each test owns its own supplier surface and doesn't see leftover state from a prior
        // test (or from production code paths exercised by another suite).
        NativeMemoryUsageTracker.setSnapshotSupplier(java.util.Collections::emptyMap);
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> 0L);
    }

    @After
    public void resetStaticsAfter() {
        NativeMemoryUsageTracker.setSnapshotSupplier(java.util.Collections::emptyMap);
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> 0L);
    }

    public void testNameMatchesEnum() {
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        assertEquals(TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER.getName(), tracker.name());
    }

    public void testRefreshEmptySnapshotIsSafe() {
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        // No supplier installed — default empty map should be picked up.
        tracker.refresh();
        Task task = createMockTaskWithResourceStats(SearchTask.class, 1, 1, randomNonNegativeLong());
        // bytesForTask must be 0 when no entry is in the snapshot.
        assertEquals(0L, tracker.bytesForTask(task));
    }

    public void testRefreshNullSnapshotFallsBackToEmpty() {
        // A misbehaving backend that returns null must not propagate NPE through the refresh path.
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> null);
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        tracker.refresh();
        Task task = createMockTaskWithResourceStats(SearchTask.class, 1, 1, 42L);
        assertEquals(0L, tracker.bytesForTask(task));
    }

    public void testBytesForTaskAfterRefreshReadsSnapshot() {
        long taskId = 12345L;
        long bytes = 8L * 1024 * 1024;
        Map<Long, Long> snapshot = new HashMap<>();
        snapshot.put(taskId, bytes);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> snapshot);
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        tracker.refresh();
        Task task = mockTaskWithId(SearchTask.class, taskId);
        assertEquals(bytes, tracker.bytesForTask(task));
    }

    public void testBytesForTaskNullTaskReturnsZero() {
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        assertEquals(0L, tracker.bytesForTask(null));
    }

    public void testHasSnapshotProviderReflectsInstallation() {
        // Once a backend installs a snapshot supplier, hasSnapshotProvider() must report true
        // even when the supplier itself returns an empty map. The contract is "is something
        // wired up", not "does the wired thing currently have data".
        Supplier<Map<Long, Long>> supplier = java.util.Collections::emptyMap;
        NativeMemoryUsageTracker.setSnapshotSupplier(supplier);
        assertTrue(NativeMemoryUsageTracker.hasSnapshotProvider());
    }

    public void testNullSnapshotSupplierIgnored() {
        // Existing supplier should be retained when caller passes null.
        Supplier<Map<Long, Long>> good = java.util.Collections::emptyMap;
        NativeMemoryUsageTracker.setSnapshotSupplier(good);
        assertTrue(NativeMemoryUsageTracker.hasSnapshotProvider());
        NativeMemoryUsageTracker.setSnapshotSupplier(null);
        // Still installed because null is a no-op.
        assertTrue(NativeMemoryUsageTracker.hasSnapshotProvider());
    }

    public void testGetNativeMemoryBudgetBytesClampsNegative() {
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> -1L);
        assertEquals(0L, NativeMemoryUsageTracker.getNativeMemoryBudgetBytes());
    }

    public void testGetNativeMemoryBudgetBytesPropagatesValue() {
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> 1024L * 1024L * 1024L);
        assertEquals(1024L * 1024L * 1024L, NativeMemoryUsageTracker.getNativeMemoryBudgetBytes());
    }

    public void testNullBudgetSupplierIgnored() {
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> 99L);
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(null);
        assertEquals(99L, NativeMemoryUsageTracker.getNativeMemoryBudgetBytes());
    }

    public void testEvaluateInertWhenFractionZero() {
        // Operator hasn't opted in (fraction=0): tracker MUST NOT cancel even with budget + heavy task.
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> 1_000L);
        Map<Long, Long> snapshot = new HashMap<>();
        long taskId = 1L;
        snapshot.put(taskId, 999L);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> snapshot);
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.0);
        tracker.refresh();
        Task task = mockTaskWithId(SearchTask.class, taskId);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }

    public void testEvaluateInertWhenBudgetZero() {
        // No backend installed budget: tracker MUST NOT cancel even with a fraction set.
        Map<Long, Long> snapshot = new HashMap<>();
        long taskId = 1L;
        snapshot.put(taskId, 999L);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> snapshot);
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        tracker.refresh();
        Task task = mockTaskWithId(SearchTask.class, taskId);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }

    public void testEvaluateNonCancellableTaskSkipped() {
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> 1_000L);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(1L, 999L));
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        tracker.refresh();
        // Plain Task (not a CancellableTask) should be ignored.
        Task task = new Task(1L, "type", "action", "description", TaskId.EMPTY_TASK_ID, java.util.Collections.emptyMap());
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }

    public void testEvaluateBelowThreshold() {
        long budget = 1_000L;
        double fraction = 0.5;
        long taskId = 7L;
        // 400 < 500 (budget*fraction)
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> budget);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 400L));
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> fraction);
        tracker.refresh();
        Task task = mockTaskWithId(SearchTask.class, taskId);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }

    public void testEvaluateAtAndAboveThresholdReturnsReason() {
        long budget = 1_000L;
        double fraction = 0.5;
        long thresholdBytes = (long) (budget * fraction);
        long taskId = 7L;
        long usage = thresholdBytes + 1L; // strictly above to keep score > 1
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> budget);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, usage));
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> fraction);
        tracker.refresh();
        Task task = mockTaskWithId(SearchShardTask.class, taskId);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertTrue("score must be at least 1", reason.get().getCancellationScore() >= 1);
        assertTrue(
            "reason message should mention native memory usage",
            reason.get().getMessage().toLowerCase(java.util.Locale.ROOT).contains("native memory")
        );
    }

    public void testEvaluateScoreScalesWithUsage() {
        // Score is floor(usage / threshold) per tracker's evaluator.
        long budget = 1_000L;
        double fraction = 0.1; // threshold = 100
        long taskId = 9L;
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> budget);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 350L)); // 3x threshold
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> fraction);
        tracker.refresh();
        Task task = mockTaskWithId(SearchTask.class, taskId);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(3, reason.get().getCancellationScore());
    }

    public void testEvaluateFractionClampedToOne() {
        // Fraction > 1.0 should be clamped to 1.0 — only tasks at or above the full budget cancel.
        long budget = 1_000L;
        long taskId = 1L;
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> budget);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 999L));
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 5.0);
        tracker.refresh();
        Task task = mockTaskWithId(SearchTask.class, taskId);
        // 999 < 1000 (clamped threshold) — must NOT cancel
        assertFalse(tracker.checkAndMaybeGetCancellationReason(task).isPresent());

        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 1_000L));
        tracker.refresh();
        // 1000 >= 1000 — cancel
        assertTrue(tracker.checkAndMaybeGetCancellationReason(task).isPresent());
    }

    public void testStatsMaxAndAvgOverActiveTasks() {
        Map<Long, Long> snapshot = new HashMap<>();
        snapshot.put(1L, 100L);
        snapshot.put(2L, 200L);
        snapshot.put(3L, 300L);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> snapshot);
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        tracker.refresh();

        List<? extends Task> tasks = List.of(
            mockTaskWithId(SearchTask.class, 1L),
            mockTaskWithId(SearchTask.class, 2L),
            mockTaskWithId(SearchTask.class, 3L)
        );
        NativeMemoryUsageTracker.Stats stats = (NativeMemoryUsageTracker.Stats) tracker.stats(tasks);
        // Cancellation count is zero — no checkAndMaybeGetCancellationReason invocations counted yet.
        NativeMemoryUsageTracker.Stats expected = new NativeMemoryUsageTracker.Stats(0L, 300L, 200L);
        assertEquals(expected, stats);
    }

    public void testStatsEmptyTaskList() {
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        tracker.refresh();
        NativeMemoryUsageTracker.Stats stats = (NativeMemoryUsageTracker.Stats) tracker.stats(List.of());
        assertEquals(new NativeMemoryUsageTracker.Stats(0L, 0L, 0L), stats);
    }

    public void testStatsWireSerializationRoundTrip() throws Exception {
        NativeMemoryUsageTracker.Stats original = new NativeMemoryUsageTracker.Stats(7L, 1024L, 512L);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NativeMemoryUsageTracker.Stats deserialized = new NativeMemoryUsageTracker.Stats(in);
                assertEquals(original, deserialized);
                assertEquals(original.hashCode(), deserialized.hashCode());
            }
        }
    }

    public void testStatsEqualsAndHashCode() {
        NativeMemoryUsageTracker.Stats a = new NativeMemoryUsageTracker.Stats(1L, 2L, 3L);
        NativeMemoryUsageTracker.Stats b = new NativeMemoryUsageTracker.Stats(1L, 2L, 3L);
        NativeMemoryUsageTracker.Stats c = new NativeMemoryUsageTracker.Stats(1L, 2L, 4L);
        assertEquals(a, a);
        assertEquals(a, b);
        assertNotEquals(a, c);
        assertNotEquals(a, null);
        assertNotEquals(a, "not a stats object");
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testIsNativeTrackingSupportedRequiresProvider() {
        // isNativeTrackingSupported() returns false when no backend has installed a real
        // supplier. Because the static supplier is a process-wide singleton, this assertion
        // is best-effort: on Linux with prior installation it may be true, but on every
        // platform it should agree with hasSnapshotProvider() AND a positive total memory
        // reading. We verify the predicate is consistent with its inputs rather than
        // hard-coding a value.
        boolean supported = NativeMemoryUsageTracker.isNativeTrackingSupported();
        if (supported) {
            assertTrue(NativeMemoryUsageTracker.hasSnapshotProvider());
            assertTrue(org.apache.lucene.util.Constants.LINUX);
        }
        // The negative direction always holds: non-Linux MUST report unsupported.
        if (org.apache.lucene.util.Constants.LINUX == false) {
            assertFalse(supported);
        }
    }

    public void testUpdateIsNoOp() {
        // update() is documented as a no-op; calling it must not throw or change snapshot state.
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(1L, 100L));
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        tracker.refresh();
        Task task = mockTaskWithId(SearchTask.class, 1L);
        tracker.update(task);
        assertEquals(100L, tracker.bytesForTask(task));
    }

    /**
     * Build a {@link CancellableTask} mock whose {@code getId()} returns the given id.
     * The test-framework helper randomizes ids, which doesn't fit a snapshot-keyed test.
     */
    private <T extends CancellableTask> T mockTaskWithId(Class<T> type, long id) {
        T task = org.mockito.Mockito.mock(type);
        org.mockito.Mockito.when(task.getId()).thenReturn(id);
        org.mockito.Mockito.when(task.getTotalResourceStats())
            .thenReturn(new org.opensearch.core.tasks.resourcetracker.TaskResourceUsage(0L, 0L));
        return task;
    }
}
