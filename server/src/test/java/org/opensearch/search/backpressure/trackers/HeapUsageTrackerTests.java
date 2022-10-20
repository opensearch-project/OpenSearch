/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

public class HeapUsageTrackerTests extends OpenSearchTestCase {
    private static final int HEAP_MOVING_AVERAGE_WINDOW_SIZE = 100;
    private static final SearchBackpressureSettings mockSettings = mock(SearchBackpressureSettings.class);
    private static final SearchShardTaskSettings mockSearchShardTaskSettings = mock(SearchShardTaskSettings.class);

    static {
        when(mockSettings.getSearchShardTaskSettings()).thenReturn(mockSearchShardTaskSettings);
        when(mockSearchShardTaskSettings.getHeapBytesThreshold()).thenReturn(100L);
        when(mockSearchShardTaskSettings.getHeapVarianceThreshold()).thenReturn(2.0);
        when(mockSearchShardTaskSettings.getHeapMovingAverageWindowSize()).thenReturn(HEAP_MOVING_AVERAGE_WINDOW_SIZE);
    }

    public void testEligibleForCancellation() {
        HeapUsageTracker tracker = new HeapUsageTracker(mockSettings);
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
        HeapUsageTracker tracker = new HeapUsageTracker(mockSettings);

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
