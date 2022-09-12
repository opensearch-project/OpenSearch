/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.HeapUsageTracker;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TaskCancellationTests extends OpenSearchTestCase {

    public void testTaskCancellation() {
        SearchShardTask mockTask = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());
        CpuUsageTracker mockCpuUsageTracker = new CpuUsageTracker(() -> 0);
        HeapUsageTracker mockHeapUsageTracker = new HeapUsageTracker(() -> 0, () -> 0.0);
        ElapsedTimeTracker mockElapsedTimeTracker = new ElapsedTimeTracker(() -> 0, () -> 0);

        List<TaskCancellation.Reason> reasons = new ArrayList<>();
        TaskCancellation taskCancellation = new TaskCancellation(mockTask, reasons, System::nanoTime);

        // Task does not have any reason to be cancelled.
        assertEquals(0, taskCancellation.totalCancellationScore());
        assertFalse(taskCancellation.isEligibleForCancellation());

        // Task has one or more reasons to be cancelled.
        reasons.add(new TaskCancellation.Reason(mockCpuUsageTracker, "cpu usage exceeded", 10));
        reasons.add(new TaskCancellation.Reason(mockHeapUsageTracker, "heap usage exceeded", 20));
        assertEquals(30, taskCancellation.totalCancellationScore());
        assertTrue(taskCancellation.isEligibleForCancellation());

        // Cancel the task and validate the cancellation reason and cancellation counters.
        taskCancellation.cancel();
        assertTrue(mockTask.getReasonCancelled().contains("[cpu usage exceeded, heap usage exceeded]"));
        assertEquals(1, mockCpuUsageTracker.getCancellations());
        assertEquals(1, mockHeapUsageTracker.getCancellations());
        assertEquals(0, mockElapsedTimeTracker.getCancellations());
    }
}
