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

public class ElapsedTimeTrackerTests extends OpenSearchTestCase {

    public void testEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 1, 100);
        ElapsedTimeTracker tracker = new ElapsedTimeTracker(() -> 200, () -> 50);

        Optional<TaskCancellation.Reason> reason = tracker.cancellationReason(task);
        assertTrue(reason.isPresent());
        assertSame(tracker, reason.get().getTracker());
        assertEquals(1, reason.get().getCancellationScore());
        assertEquals("elapsed time exceeded", reason.get().getMessage());
    }

    public void testNotEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 1, 100);
        ElapsedTimeTracker tracker = new ElapsedTimeTracker(() -> 200, () -> 200);

        Optional<TaskCancellation.Reason> reason = tracker.cancellationReason(task);
        assertFalse(reason.isPresent());
    }
}
