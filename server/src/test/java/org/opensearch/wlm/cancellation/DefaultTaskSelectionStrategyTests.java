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
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerServiceTests.TestClock;
import org.opensearch.wlm.tracker.TaskResourceUsageCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.wlm.cancellation.DefaultTaskCancellation.MIN_VALUE;
import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.HEAP_SIZE_BYTES;

public class DefaultTaskSelectionStrategyTests extends OpenSearchTestCase {
    private TestClock clock;

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsGreaterThanZero() {
        clock = new TestClock();
        DefaultTaskSelectionStrategy testDefaultTaskSelectionStrategy = new DefaultTaskSelectionStrategy(clock::getTime);
        long thresholdInLong = 100L;
        double reduceBy = 50.0 / HEAP_SIZE_BYTES;
        ResourceType resourceType = ResourceType.MEMORY;
        List<QueryGroupTask> tasks = getListOfTasks(thresholdInLong);
        List<QueryGroupTask> selectedTasks = testDefaultTaskSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
        assertFalse(selectedTasks.isEmpty());
        assertTrue(tasksUsageMeetsThreshold(selectedTasks, reduceBy));
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsLesserThanZero() {
        DefaultTaskSelectionStrategy testDefaultTaskSelectionStrategy = new DefaultTaskSelectionStrategy();
        long thresholdInLong = 100L;
        double reduceBy = -50.0 / HEAP_SIZE_BYTES;
        ResourceType resourceType = ResourceType.MEMORY;
        List<QueryGroupTask> tasks = getListOfTasks(thresholdInLong);
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
        double reduceBy = 0.0;
        ResourceType resourceType = ResourceType.MEMORY;
        List<QueryGroupTask> tasks = getListOfTasks(thresholdInLong);
        List<QueryGroupTask> selectedTasks = testDefaultTaskSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
        assertTrue(selectedTasks.isEmpty());
    }

    private boolean tasksUsageMeetsThreshold(List<QueryGroupTask> selectedTasks, double threshold) {
        double memory = 0;
        for (Task task : selectedTasks) {
            memory += TaskResourceUsageCalculator.from(ResourceType.MEMORY).calculateFor(task, clock::getTime);
            if ((memory - threshold) > MIN_VALUE) {
                return true;
            }
        }
        return false;
    }

    private List<QueryGroupTask> getListOfTasks(long totalMemory) {
        List<QueryGroupTask> tasks = new ArrayList<>();

        while (totalMemory > 0) {
            long id = randomLong();
            final QueryGroupTask task = getRandomSearchTask(id);
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

    private QueryGroupTask getRandomSearchTask(long id) {
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
