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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

public class HeapUsageTrackerTests extends OpenSearchTestCase {
    private static final long HEAP_BYTES_THRESHOLD = 100;
    private static final long HEAP_BYTES_THRESHOLD_FOR_SEARCH_QUERY = 50;
    private static final int HEAP_MOVING_AVERAGE_WINDOW_SIZE = 100;

    private static final SearchBackpressureSettings mockSettings = new SearchBackpressureSettings(
        Settings.builder()
            .put(HeapUsageTracker.SETTING_HEAP_VARIANCE_THRESHOLD_FOR_SEARCH_QUERY.getKey(), 3.0)
            .put(HeapUsageTracker.SETTING_HEAP_VARIANCE_THRESHOLD.getKey(), 2.0)
            .put(HeapUsageTracker.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE_FOR_SEARCH_QUERY.getKey(), HEAP_MOVING_AVERAGE_WINDOW_SIZE)
            .put(HeapUsageTracker.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE.getKey(), HEAP_MOVING_AVERAGE_WINDOW_SIZE)
            .build(),
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    public void testSearchTaskEligibleForCancellation() {
        HeapUsageTracker tracker = spy(new HeapUsageTracker(mockSettings));
        when(tracker.getHeapBytesThresholdForSearchQuery()).thenReturn(HEAP_BYTES_THRESHOLD_FOR_SEARCH_QUERY);
        Task task = createMockTaskWithResourceStats(SearchTask.class, 1, 50);

        // Record enough observations to make the moving average 'ready'.
        for (int i = 0; i < HEAP_MOVING_AVERAGE_WINDOW_SIZE; i++) {
            tracker.update(task);
        }

        // Task that has heap usage >= heapBytesThreshold and (movingAverage * heapVariance).
        task = createMockTaskWithResourceStats(SearchTask.class, 1, 300);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(6, reason.get().getCancellationScore());
        assertEquals("heap usage exceeded [300b >= 150b]", reason.get().getMessage());
    }

    public void testSearchShardTaskEligibleForCancellation() {
        HeapUsageTracker tracker = spy(new HeapUsageTracker(mockSettings));
        when(tracker.getHeapBytesThreshold()).thenReturn(HEAP_BYTES_THRESHOLD);
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 50);

        // Record enough observations to make the moving average 'ready'.
        for (int i = 0; i < HEAP_MOVING_AVERAGE_WINDOW_SIZE; i++) {
            tracker.update(task);
        }

        // Task that has heap usage >= heapBytesThreshold and (movingAverage * heapVariance).
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 200);
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(4, reason.get().getCancellationScore());
        assertEquals("heap usage exceeded [200b >= 100b]", reason.get().getMessage());
    }

    public void testNotEligibleForCancellation() {
        Task task;
        Optional<TaskCancellation.Reason> reason;
        HeapUsageTracker tracker = spy(new HeapUsageTracker(mockSettings));
        when(tracker.getHeapBytesThreshold()).thenReturn(HEAP_BYTES_THRESHOLD);

        // Task with heap usage < heapBytesThreshold.
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 99);

        // Not enough observations.
        reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());

        // Record enough observations to make the moving average 'ready'.
        for (int i = 0; i < HEAP_MOVING_AVERAGE_WINDOW_SIZE; i++) {
            tracker.update(task);
        }

        // Task with heap usage < heapBytesThreshold should not be cancelled.
        reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());

        // Task with heap usage between heapBytesThreshold and (movingAverage * heapVariance) should not be cancelled.
        double allowedHeapUsage = 99.0 * 2.0;
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, randomLongBetween(99, (long) allowedHeapUsage - 1));
        reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }
}
