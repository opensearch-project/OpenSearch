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
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.tracker.ResourceUsageCalculatorTrackerServiceTests.TestClock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.opensearch.wlm.cancellation.TaskCanceller.MIN_VALUE;
import static org.opensearch.wlm.tracker.MemoryUsageCalculator.HEAP_SIZE_BYTES;

public class HighestResourceConsumingTaskFirstSelectionStrategyTests extends OpenSearchTestCase {
    private TestClock clock;

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsGreaterThanZero() {
        clock = new TestClock();
        HighestResourceConsumingTaskFirstSelectionStrategy testHighestResourceConsumingTaskFirstSelectionStrategy =
            new HighestResourceConsumingTaskFirstSelectionStrategy(clock::getTime);
        double reduceBy = 50000.0 / HEAP_SIZE_BYTES;
        ResourceType resourceType = ResourceType.MEMORY;
        List<QueryGroupTask> tasks = getListOfTasks(100);
        List<QueryGroupTask> selectedTasks = testHighestResourceConsumingTaskFirstSelectionStrategy.selectTasksForCancellation(
            tasks,
            reduceBy,
            resourceType
        );
        assertFalse(selectedTasks.isEmpty());
        boolean sortedInDescendingResourceUsage = IntStream.range(0, selectedTasks.size() - 1)
            .noneMatch(
                index -> ResourceType.MEMORY.getResourceUsageCalculator()
                    .calculateTaskResourceUsage(selectedTasks.get(index), null) < ResourceType.MEMORY.getResourceUsageCalculator()
                        .calculateTaskResourceUsage(selectedTasks.get(index + 1), null)
            );
        assertTrue(sortedInDescendingResourceUsage);
        assertTrue(tasksUsageMeetsThreshold(selectedTasks, reduceBy));
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsLesserThanZero() {
        HighestResourceConsumingTaskFirstSelectionStrategy testHighestResourceConsumingTaskFirstSelectionStrategy =
            new HighestResourceConsumingTaskFirstSelectionStrategy();
        double reduceBy = -50.0 / HEAP_SIZE_BYTES;
        ResourceType resourceType = ResourceType.MEMORY;
        List<QueryGroupTask> tasks = getListOfTasks(3);
        try {
            testHighestResourceConsumingTaskFirstSelectionStrategy.selectTasksForCancellation(tasks, reduceBy, resourceType);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("limit has to be greater than zero", e.getMessage());
        }
    }

    public void testSelectTasksToCancelSelectsTasksMeetingThreshold_ifReduceByIsEqualToZero() {
        HighestResourceConsumingTaskFirstSelectionStrategy testHighestResourceConsumingTaskFirstSelectionStrategy =
            new HighestResourceConsumingTaskFirstSelectionStrategy();
        double reduceBy = 0.0;
        ResourceType resourceType = ResourceType.MEMORY;
        List<QueryGroupTask> tasks = getListOfTasks(50);
        List<QueryGroupTask> selectedTasks = testHighestResourceConsumingTaskFirstSelectionStrategy.selectTasksForCancellation(
            tasks,
            reduceBy,
            resourceType
        );
        assertTrue(selectedTasks.isEmpty());
    }

    private boolean tasksUsageMeetsThreshold(List<QueryGroupTask> selectedTasks, double threshold) {
        double memory = 0;
        for (QueryGroupTask task : selectedTasks) {
            memory += ResourceType.MEMORY.getResourceUsageCalculator().calculateTaskResourceUsage(task, clock::getTime);
            if ((memory - threshold) > MIN_VALUE) {
                return true;
            }
        }
        return false;
    }

    private List<QueryGroupTask> getListOfTasks(int numberOfTasks) {
        List<QueryGroupTask> tasks = new ArrayList<>();

        while (tasks.size() < numberOfTasks) {
            long id = randomLong();
            final QueryGroupTask task = getRandomSearchTask(id);
            long initial_memory = randomLongBetween(1, 100);

            ResourceUsageMetric[] initialTaskResourceMetrics = new ResourceUsageMetric[] {
                new ResourceUsageMetric(ResourceStats.MEMORY, initial_memory) };
            task.startThreadResourceTracking(id, ResourceStatsType.WORKER_STATS, initialTaskResourceMetrics);

            long memory = initial_memory + randomLongBetween(1, 10000);

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
