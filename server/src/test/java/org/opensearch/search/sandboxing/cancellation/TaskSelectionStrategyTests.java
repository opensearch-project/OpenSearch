/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.cancellation;

import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Comparator;
import java.util.List;

public class TaskSelectionStrategyTests extends OpenSearchTestCase {

    public static class TestTaskSelectionStrategy extends AbstractTaskSelectionStrategy {
        @Override
        public Comparator<Task> sortingCondition() {
            return Comparator.comparingLong(Task::getStartTime).reversed();
        }
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsGreaterThanZero() {
        TaskSelectionStrategy testTaskSelectionStrategy = new TestTaskSelectionStrategy();
        long threshold = 100L;
        long reduceBy = 50L;
        SandboxResourceType resourceType = SandboxResourceType.fromString("JVM");
        List<Task> tasks = SandboxTestHelpers.getListOfTasks(threshold);

        List<Task> selectedTasks = testTaskSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
        assertFalse(selectedTasks.isEmpty());
        assertTrue(tasksUsageMeetsThreshold(selectedTasks, reduceBy));
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsLesserThanZero() {
        TaskSelectionStrategy testTaskSelectionStrategy = new TestTaskSelectionStrategy();
        long threshold = 100L;
        long reduceBy = -50L;
        SandboxResourceType resourceType = SandboxResourceType.fromString("JVM");
        List<Task> tasks = SandboxTestHelpers.getListOfTasks(threshold);

        try {
            testTaskSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("reduceBy has to be greater than zero", e.getMessage());
        }
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsEqualToZero() {
        TaskSelectionStrategy testTaskSelectionStrategy = new TestTaskSelectionStrategy();
        long threshold = 100L;
        long reduceBy = 0;
        SandboxResourceType resourceType = SandboxResourceType.fromString("JVM");
        List<Task> tasks = SandboxTestHelpers.getListOfTasks(threshold);

        List<Task> selectedTasks = testTaskSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
        assertTrue(selectedTasks.isEmpty());
    }

    private boolean tasksUsageMeetsThreshold(List<Task> selectedTasks, long threshold) {
        long memory = 0;
        for (Task task : selectedTasks) {
            memory += task.getTotalResourceUtilization(ResourceStats.MEMORY);
            if (memory > threshold) {
                return true;
            }
        }
        return false;
    }
}
