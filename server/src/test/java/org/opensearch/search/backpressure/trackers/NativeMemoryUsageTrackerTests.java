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
import org.opensearch.search.backpressure.NativeMemoryUsageService;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Unit tests for {@link NativeMemoryUsageTracker}.
 *
 * <p>The tracker is a thin wrapper over {@link NativeMemoryUsageService}, which is a
 * process-wide singleton. The service holds the snapshot supplier and budget supplier
 * that backend plugins install at boot. Each test must reset the singleton in
 * {@link #setUp()}/{@link #tearDown()} so leftover suppliers from a prior test (or from
 * production code paths exercised by another suite) don't leak.
 */
public class NativeMemoryUsageTrackerTests extends OpenSearchTestCase {

    @Before
    public void resetServiceBefore() {
        NativeMemoryUsageService.getInstance().resetForTesting();
    }

    @After
    public void resetServiceAfter() {
        NativeMemoryUsageService.getInstance().resetForTesting();
    }

    public void testNameMatchesEnum() {
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        assertEquals(TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER.getName(), tracker.name());
    }

    public void testBytesForTaskWithEmptySnapshot() {
        // No supplier installed — the service's default empty snapshot must yield 0 for any task id.
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        Task task = createMockTask(SearchTask.class, randomNonNegativeLong());
        assertEquals(0L, tracker.bytesForTask(task));
    }

    public void testBytesForTaskAfterServiceRefresh() {
        long taskId = 12345L;
        long bytes = 8L * 1024 * 1024;
        Map<Long, Long> snapshot = new HashMap<>();
        snapshot.put(taskId, bytes);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> snapshot);

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        // Refresh is now owned by the service; the tracker reads what the service publishes.
        NativeMemoryUsageService.getInstance().refresh();

        Task task = createMockTask(SearchTask.class, taskId);
        assertEquals(bytes, tracker.bytesForTask(task));
    }

    public void testBytesForTaskNullTaskReturnsZero() {
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        assertEquals(0L, tracker.bytesForTask(null));
    }

    public void testHasSnapshotProviderReflectsInstallation() {
        // Before any backend installs a supplier, the predicate is false.
        assertFalse(NativeMemoryUsageTracker.hasSnapshotProvider());

        // Once installed, hasSnapshotProvider must return true even when the supplier itself
        // returns an empty map. Contract is "is something wired up?", not "does it have data?".
        NativeMemoryUsageTracker.setSnapshotSupplier(Collections::emptyMap);
        assertTrue(NativeMemoryUsageTracker.hasSnapshotProvider());
    }

    public void testNullSnapshotSupplierIgnored() {
        Supplier<Map<Long, Long>> good = Collections::emptyMap;
        NativeMemoryUsageTracker.setSnapshotSupplier(good);
        assertTrue(NativeMemoryUsageTracker.hasSnapshotProvider());
        // Passing null must not erase a working installation.
        NativeMemoryUsageTracker.setSnapshotSupplier(null);
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
        long taskId = 1L;
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 999L));
        NativeMemoryUsageService.getInstance().refresh();

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.0);
        Task task = createMockTask(SearchTask.class, taskId);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }

    public void testEvaluateInertWhenBudgetZero() {
        // No backend installed budget: tracker MUST NOT cancel even with a fraction set.
        long taskId = 1L;
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 999L));
        NativeMemoryUsageService.getInstance().refresh();

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        Task task = createMockTask(SearchTask.class, taskId);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }

    public void testEvaluateNonCancellableTaskSkipped() {
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> 1_000L);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(1L, 999L));
        NativeMemoryUsageService.getInstance().refresh();

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        // Plain Task (not a CancellableTask) must be ignored by the evaluator.
        Task task = new Task(1L, "type", "action", "description", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }

    public void testEvaluateBelowThresholdNoCancellation() {
        long budget = 1_000L;
        double fraction = 0.5;
        long taskId = 7L;
        // 400 < 500 (budget * fraction)
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> budget);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 400L));
        NativeMemoryUsageService.getInstance().refresh();

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> fraction);
        Task task = createMockTask(SearchTask.class, taskId);
        assertFalse(tracker.checkAndMaybeGetCancellationReason(task).isPresent());
    }

    public void testEvaluateAtAndAboveThresholdReturnsReason() {
        long budget = 1_000L;
        double fraction = 0.5;
        long thresholdBytes = (long) (budget * fraction);
        long taskId = 7L;
        long usage = thresholdBytes + 1L;
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> budget);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, usage));
        NativeMemoryUsageService.getInstance().refresh();

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> fraction);
        Task task = createMockTask(SearchShardTask.class, taskId);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue("usage above threshold must yield a cancellation reason", reason.isPresent());
        assertTrue("score must be at least 1", reason.get().getCancellationScore() >= 1);
        assertTrue(
            "reason message should mention native memory",
            reason.get().getMessage().toLowerCase(java.util.Locale.ROOT).contains("native memory")
        );
    }

    public void testEvaluateScoreScalesWithUsage() {
        // Score is floor(usage / threshold) per the tracker's evaluator.
        long budget = 1_000L;
        double fraction = 0.1; // threshold = 100
        long taskId = 9L;
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> budget);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 350L)); // 3x threshold
        NativeMemoryUsageService.getInstance().refresh();

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> fraction);
        Task task = createMockTask(SearchTask.class, taskId);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(3, reason.get().getCancellationScore());
    }

    public void testEvaluateFractionClampedToOne() {
        // Fraction > 1.0 must be clamped to 1.0 — only tasks at or above the full budget cancel.
        long budget = 1_000L;
        long taskId = 1L;
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> budget);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 999L));
        NativeMemoryUsageService.getInstance().refresh();

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 5.0);
        Task task = createMockTask(SearchTask.class, taskId);
        // 999 < 1000 (clamped threshold) — must NOT cancel.
        assertFalse(tracker.checkAndMaybeGetCancellationReason(task).isPresent());

        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(taskId, 1_000L));
        NativeMemoryUsageService.getInstance().refresh();
        // 1000 >= 1000 — must cancel.
        assertTrue(tracker.checkAndMaybeGetCancellationReason(task).isPresent());
    }

    public void testStatsMaxAndAvgOverActiveTasks() {
        Map<Long, Long> snapshot = new HashMap<>();
        snapshot.put(1L, 100L);
        snapshot.put(2L, 200L);
        snapshot.put(3L, 300L);
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> snapshot);
        NativeMemoryUsageService.getInstance().refresh();

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        List<? extends Task> tasks = List.of(
            createMockTask(SearchTask.class, 1L),
            createMockTask(SearchTask.class, 2L),
            createMockTask(SearchTask.class, 3L)
        );
        NativeMemoryUsageTracker.Stats stats = (NativeMemoryUsageTracker.Stats) tracker.stats(tasks);
        // No checkAndMaybeGetCancellationReason invocations — cancellation count stays at zero.
        NativeMemoryUsageTracker.Stats expected = new NativeMemoryUsageTracker.Stats(0L, 300L, 200L);
        assertEquals(expected, stats);
    }

    public void testStatsEmptyTaskList() {
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
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

    public void testIsNativeTrackingSupportedNonLinuxAlwaysFalse() {
        // Negative direction always holds: non-Linux MUST report unsupported regardless of state.
        if (org.apache.lucene.util.Constants.LINUX == false) {
            assertFalse(NativeMemoryUsageTracker.isNativeTrackingSupported());
        }
    }

    public void testIsNativeTrackingSupportedRequiresProvider() {
        // Without a snapshot provider the predicate must be false even on Linux.
        // (resetForTesting() in @Before clears any previously installed supplier.)
        assertFalse(NativeMemoryUsageTracker.isNativeTrackingSupported());
    }

    public void testUpdateIsNoOp() {
        // update() is documented as a no-op; calling it must not throw or change snapshot state.
        NativeMemoryUsageTracker.setSnapshotSupplier(() -> Map.of(1L, 100L));
        NativeMemoryUsageService.getInstance().refresh();

        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5);
        Task task = createMockTask(SearchTask.class, 1L);
        tracker.update(task);
        assertEquals(100L, tracker.bytesForTask(task));
    }

    public void testTrackerAcceptsInjectedService() {
        // Package-private constructor lets a test isolate the service from the singleton.
        NativeMemoryUsageService isolated = NativeMemoryUsageService.getInstance();
        NativeMemoryUsageTracker tracker = new NativeMemoryUsageTracker(() -> 0.5, isolated);
        assertNotNull(tracker);
        assertEquals(TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER.getName(), tracker.name());
    }

    /**
     * Build a {@link CancellableTask} mock whose {@code getId()} returns the given id. The
     * {@link org.opensearch.search.backpressure.SearchBackpressureTestHelpers#createMockTaskWithResourceStats}
     * helper randomizes the id, which doesn't fit a snapshot-keyed test where evaluator
     * lookups must hit a specific entry.
     */
    private <T extends CancellableTask> T createMockTask(Class<T> type, long id) {
        T task = org.mockito.Mockito.mock(type);
        org.mockito.Mockito.when(task.getId()).thenReturn(id);
        org.mockito.Mockito.when(task.getTotalResourceStats())
            .thenReturn(new org.opensearch.core.tasks.resourcetracker.TaskResourceUsage(0L, 0L));
        return task;
    }
}
