/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchTask;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.search.ResourceType;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DefaultTaskSelectionStrategyTests extends OpenSearchTestCase {

    public void testSelectTasksFromDeletedQueryGroup() {
        DefaultTaskSelectionStrategy testDefaultTaskSelectionStrategy = new DefaultTaskSelectionStrategy();
        long thresholdInLong = 100L;
        long reduceBy = Long.MIN_VALUE;
        List<Task> tasks = getListOfTasks(thresholdInLong);
        List<Task> selectedTasks = testDefaultTaskSelectionStrategy.selectTasksFromDeletedQueryGroup(tasks);
        assertFalse(selectedTasks.isEmpty());
        assertTrue(tasksUsageMeetsThreshold(selectedTasks, reduceBy));
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsGreaterThanZero() {
        DefaultTaskSelectionStrategy testDefaultTaskSelectionStrategy = new DefaultTaskSelectionStrategy();
        long thresholdInLong = 100L;
        long reduceBy = 50L;
        ResourceType resourceType = ResourceType.MEMORY;
        List<Task> tasks = getListOfTasks(thresholdInLong);
        List<Task> selectedTasks = testDefaultTaskSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
        assertFalse(selectedTasks.isEmpty());
        assertTrue(tasksUsageMeetsThreshold(selectedTasks, reduceBy));
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsLesserThanZero() {
        DefaultTaskSelectionStrategy testDefaultTaskSelectionStrategy = new DefaultTaskSelectionStrategy();
        long thresholdInLong = 100L;
        long reduceBy = -50L;
        ResourceType resourceType = ResourceType.MEMORY;
        List<Task> tasks = getListOfTasks(thresholdInLong);
        try {
            testDefaultTaskSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("limit has to be greater than zero", e.getMessage());
        }
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsEqualToZero() {
        DefaultTaskSelectionStrategy testDefaultTaskSelectionStrategy = new DefaultTaskSelectionStrategy();
        long thresholdInLong = 100L;
        long reduceBy = 0;
        ResourceType resourceType = ResourceType.MEMORY;
        List<Task> tasks = getListOfTasks(thresholdInLong);
        List<Task> selectedTasks = testDefaultTaskSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
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

    private List<Task> getListOfTasks(long totalMemory) {
        List<Task> tasks = new ArrayList<>();

        while (totalMemory > 0) {
            long id = randomLong();
            final Task task = getRandomSearchTask(id);
            long initial_memory = randomLongBetween(1, 100);

            ResourceUsageMetric[] initialTaskResourceMetrics = new ResourceUsageMetric[] {
                new ResourceUsageMetric(ResourceStats.MEMORY, initial_memory) };
            task.startThreadResourceTracking(id, ResourceStatsType.WORKER_STATS, initialTaskResourceMetrics);

            long memory = initial_memory + randomLongBetween(1, 10000);

            totalMemory -= memory - initial_memory;

            ResourceUsageMetric[] taskResourceMetrics = new ResourceUsageMetric[] {
                new ResourceUsageMetric(ResourceStats.MEMORY, memory), };
            task.updateThreadResourceStats(id, ResourceStatsType.WORKER_STATS, taskResourceMetrics);
            task.stopThreadResourceTracking(id, ResourceStatsType.WORKER_STATS);
            tasks.add(task);
        }

        return tasks;
    }

    private Task getRandomSearchTask(long id) {
        return new SearchTask(
            id,
            "transport",
            SearchAction.NAME,
            () -> "test description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );
    }
}
