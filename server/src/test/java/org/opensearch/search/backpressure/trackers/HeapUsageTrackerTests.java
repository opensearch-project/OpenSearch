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
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.settings.SearchTaskSettings;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class HeapUsageTrackerTests extends OpenSearchTestCase {
    private static final long HEAP_BYTES_THRESHOLD_SEARCH_SHARD_TASK = 100;
    private static final long HEAP_BYTES_THRESHOLD_SEARCH_TASK = 50;
    private static final int HEAP_MOVING_AVERAGE_WINDOW_SIZE = 100;

    private static final SearchBackpressureSettings mockSettings = new SearchBackpressureSettings(
        Settings.builder()
            .put(SearchTaskSettings.SETTING_HEAP_VARIANCE_THRESHOLD.getKey(), 3.0)
            .put(SearchShardTaskSettings.SETTING_HEAP_VARIANCE_THRESHOLD.getKey(), 2.0)
            .put(SearchTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE.getKey(), HEAP_MOVING_AVERAGE_WINDOW_SIZE)
            .put(SearchShardTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE.getKey(), HEAP_MOVING_AVERAGE_WINDOW_SIZE)
            .build(),
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    public void testSearchTaskEligibleForCancellation() {
        SearchTaskSettings mockSearchTaskSettings = spy(
            new SearchTaskSettings(mockSettings.getSettings(), mockSettings.getClusterSettings())
        );
        // setting the heap percent threshold to minimum
        when(mockSearchTaskSettings.getHeapPercentThreshold()).thenReturn(0.0);
        HeapUsageTracker tracker = spy(
            new HeapUsageTracker(
                mockSearchTaskSettings::getHeapVarianceThreshold,
                mockSearchTaskSettings::getHeapPercentThreshold,
                mockSearchTaskSettings.getHeapMovingAverageWindowSize(),
                mockSettings.getClusterSettings(),
                SearchTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE
            )
        );
        Task task = createMockTaskWithResourceStats(SearchTask.class, 1, 50, randomNonNegativeLong());

        // Record enough observations to make the moving average 'ready'.
        for (int i = 0; i < HEAP_MOVING_AVERAGE_WINDOW_SIZE; i++) {
            tracker.update(task);
        }

        // Task that has heap usage >= heapBytesThreshold and (movingAverage * heapVariance).
        task = createMockTaskWithResourceStats(SearchTask.class, 1, 300, randomNonNegativeLong());
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(6, reason.get().getCancellationScore());
        assertEquals("heap usage exceeded [300b >= 150b]", reason.get().getMessage());
    }

    public void testSearchShardTaskEligibleForCancellation() {
        SearchShardTaskSettings mockSearchShardTaskSettings = spy(
            new SearchShardTaskSettings(mockSettings.getSettings(), mockSettings.getClusterSettings())
        );
        // setting the heap percent threshold to minimum
        when(mockSearchShardTaskSettings.getHeapPercentThreshold()).thenReturn(0.0);
        HeapUsageTracker tracker = spy(
            new HeapUsageTracker(
                mockSearchShardTaskSettings::getHeapVarianceThreshold,
                mockSearchShardTaskSettings::getHeapPercentThreshold,
                mockSearchShardTaskSettings.getHeapMovingAverageWindowSize(),
                mockSettings.getClusterSettings(),
                SearchShardTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE
            )
        );
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 50, randomNonNegativeLong());

        // Record enough observations to make the moving average 'ready'.
        for (int i = 0; i < HEAP_MOVING_AVERAGE_WINDOW_SIZE; i++) {
            tracker.update(task);
        }

        // Task that has heap usage >= heapBytesThreshold and (movingAverage * heapVariance).
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 200, randomNonNegativeLong());
        Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertTrue(reason.isPresent());
        assertEquals(4, reason.get().getCancellationScore());
        assertEquals("heap usage exceeded [200b >= 100b]", reason.get().getMessage());
    }

    public void testNotEligibleForCancellation() {
        Task task;
        Optional<TaskCancellation.Reason> reason;
        SearchShardTaskSettings mockSearchShardTaskSettings = spy(
            new SearchShardTaskSettings(mockSettings.getSettings(), mockSettings.getClusterSettings())
        );
        // setting the heap percent threshold to minimum
        when(mockSearchShardTaskSettings.getHeapPercentThreshold()).thenReturn(0.0);
        HeapUsageTracker tracker = spy(
            new HeapUsageTracker(
                mockSearchShardTaskSettings::getHeapVarianceThreshold,
                mockSearchShardTaskSettings::getHeapPercentThreshold,
                mockSearchShardTaskSettings.getHeapMovingAverageWindowSize(),
                mockSettings.getClusterSettings(),
                SearchShardTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE
            )
        );

        // Task with heap usage < heapBytesThreshold.
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 99, randomNonNegativeLong());

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
        task = createMockTaskWithResourceStats(
            SearchShardTask.class,
            1,
            randomLongBetween(99, (long) allowedHeapUsage - 1),
            randomNonNegativeLong()
        );
        reason = tracker.checkAndMaybeGetCancellationReason(task);
        assertFalse(reason.isPresent());
    }

    public void testIsHeapUsageDominatedBySearch() {
        assumeTrue("Skip the test if the hardware doesn't support heap usage tracking", HeapUsageTracker.isHeapTrackingSupported());

        // task with 1 byte of heap usage so that it does not breach the threshold
        CancellableTask task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 1, randomNonNegativeLong());
        assertFalse(HeapUsageTracker.isHeapUsageDominatedBySearch(List.of(task), 0.5));

        long totalHeap = JvmStats.jvmStats().getMem().getHeapMax().getBytes();
        // task with heap usage of [totalHeap - 1] so that it breaches the threshold
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, totalHeap - 1, randomNonNegativeLong());
        assertTrue(HeapUsageTracker.isHeapUsageDominatedBySearch(List.of(task), 0.5));
    }
}
