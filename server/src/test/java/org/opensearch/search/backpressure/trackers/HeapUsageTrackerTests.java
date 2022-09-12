/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

public class HeapUsageTrackerTests extends OpenSearchTestCase {

    public void testEligibleForCancellation() {
        HeapUsageTracker tracker = new HeapUsageTracker(() -> 100L, () -> 2.0);
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 50);

        // Record enough observations to make the moving average 'ready'.
        for (int i = 0; i < 100; i++) {
            tracker.update(task);
        }

        // Task that has heap usage >= searchTaskHeapThresholdBytes and (moving average * variance).
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 200);
        Optional<TaskCancellation.Reason> reason = tracker.cancellationReason(task);
        assertTrue(reason.isPresent());
        assertSame(tracker, reason.get().getTracker());
        assertEquals(4, reason.get().getCancellationScore());
        assertEquals("heap usage exceeded", reason.get().getMessage());
    }

    public void testNotEligibleForCancellation() {
        Task task;
        Optional<TaskCancellation.Reason> reason;
        HeapUsageTracker tracker = new HeapUsageTracker(() -> 100L, () -> 2.0);

        // Task with heap usage < searchTaskHeapThresholdBytes.
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 99);

        // Not enough observations.
        reason = tracker.cancellationReason(task);
        assertFalse(reason.isPresent());

        // Record enough observations to make the moving average 'ready'.
        for (int i = 0; i < 100; i++) {
            tracker.update(task);
        }

        // Task with heap usage < searchTaskHeapThresholdBytes should not be cancelled.
        reason = tracker.cancellationReason(task);
        assertFalse(reason.isPresent());

        // Task with heap usage between [searchTaskHeapThresholdBytes, moving average * variance) should not be cancelled.
        double allowedHeapUsage = 99.0 * 2.0;
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, randomLongBetween(99, (long) allowedHeapUsage - 1));
        reason = tracker.cancellationReason(task);
        assertFalse(reason.isPresent());
    }
}
