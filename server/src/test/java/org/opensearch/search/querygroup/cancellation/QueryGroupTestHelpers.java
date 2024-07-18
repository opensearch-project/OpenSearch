/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup.cancellation;

import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.search.querygroup.QueryGroupLevelResourceUsageView;
import org.opensearch.search.resourcetypes.ResourceType;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.mockito.Mockito;

import static org.opensearch.test.OpenSearchTestCase.randomLong;
import static org.opensearch.test.OpenSearchTestCase.randomLongBetween;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryGroupTestHelpers {

    public static List<Task> getListOfTasks(long totalMemory) {
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

    public static Task getRandomTask(long id) {
        return new Task(
            id,
            "transport",
            SearchAction.NAME,
            "test description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );
    }

    public static Task getRandomSearchTask(long id) {
        return new SearchTask(
            id,
            "transport",
            SearchAction.NAME,
            () -> "test description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );
    }

    public static QueryGroup createQueryGroupMock(String id, String resourceTypeStr, Long threshold, Long usage) {
        QueryGroup queryGroupMock = Mockito.mock(QueryGroup.class);
        when(queryGroupMock.get_id()).thenReturn(id);

        ResourceType resourceType = ResourceType.fromName(resourceTypeStr);
        when(queryGroupMock.getResourceLimits()).thenReturn(Map.of(resourceType, threshold));
        when(queryGroupMock.getThresholdInLong(resourceType)).thenReturn(threshold);
        return queryGroupMock;
    }

    public static QueryGroupLevelResourceUsageView createResourceUsageViewMock(String resourceTypeStr, Long usage) {
        QueryGroupLevelResourceUsageView mockView = mock(QueryGroupLevelResourceUsageView.class);
        ResourceType resourceType = ResourceType.fromName(resourceTypeStr);
        when(mockView.getResourceUsageData()).thenReturn(Collections.singletonMap(resourceType, usage));
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(1234), getRandomSearchTask(4321)));
        return mockView;
    }

    private static Supplier<String> descriptionSupplier() {
        return () -> "test description";
    }
}
