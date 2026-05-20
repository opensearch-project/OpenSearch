/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TaskCancellationTests extends OpenSearchTestCase {

    public void testTaskCancellation() {
        SearchShardTask mockTask = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());

        TaskResourceUsageTracker mockTracker1 = createMockTaskResourceUsageTracker("mock_tracker_1");
        TaskResourceUsageTracker mockTracker2 = createMockTaskResourceUsageTracker("mock_tracker_2");
        TaskResourceUsageTracker mockTracker3 = createMockTaskResourceUsageTracker("mock_tracker_3");

        List<TaskCancellation.Reason> reasons = new ArrayList<>();
        List<Runnable> callbacks = List.of(mockTracker1::incrementCancellations, mockTracker2::incrementCancellations);
        TaskCancellation taskCancellation = new TaskCancellation(mockTask, reasons, callbacks);

        // Task does not have any reason to be cancelled.
        assertEquals(0, taskCancellation.totalCancellationScore());
        assertFalse(taskCancellation.isEligibleForCancellation());
        taskCancellation.cancel();
        assertEquals(0, mockTracker1.getCancellations());
        assertEquals(0, mockTracker2.getCancellations());
        assertEquals(0, mockTracker3.getCancellations());

        // Task has one or more reasons to be cancelled.
        reasons.add(new TaskCancellation.Reason("limits exceeded 1", 10));
        reasons.add(new TaskCancellation.Reason("limits exceeded 2", 20));
        reasons.add(new TaskCancellation.Reason("limits exceeded 3", 5));
        assertEquals(35, taskCancellation.totalCancellationScore());
        assertTrue(taskCancellation.isEligibleForCancellation());

        // Cancel the task and validate the cancellation reason and invocation of callbacks.
        taskCancellation.cancel();
        assertTrue(mockTask.getReasonCancelled().contains("limits exceeded 1, limits exceeded 2, limits exceeded 3"));
        assertEquals(1, mockTracker1.getCancellations());
        assertEquals(1, mockTracker2.getCancellations());
        assertEquals(0, mockTracker3.getCancellations());
    }

    private static TaskResourceUsageTracker createMockTaskResourceUsageTracker(String name) {
        return new TaskResourceUsageTracker() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public void update(Task task) {}

            @Override
            public Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task) {
                return Optional.empty();
            }

            @Override
            public TaskResourceUsageTracker.Stats stats(List<? extends Task> activeTasks) {
                return null;
            }
        };
    }
}
