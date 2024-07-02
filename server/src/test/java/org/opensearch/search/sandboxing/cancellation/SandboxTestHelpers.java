/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.cancellation;

import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.metadata.Sandbox;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.search.sandboxing.SandboxLevelResourceUsageView;
import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.mockito.Mockito;

import static org.opensearch.test.OpenSearchTestCase.randomLong;
import static org.opensearch.test.OpenSearchTestCase.randomLongBetween;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SandboxTestHelpers {

    public static List<Task> getListOfTasks(long totalMemory) {
        List<Task> tasks = new ArrayList<>();

        while (totalMemory > 0) {
            long id = randomLong();
            final Task task = getRandomTask(id);
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

    public static Sandbox createSandboxMock(String id, String resourceTypeStr, Long threshold, Long usage) {
        Sandbox sandbox = Mockito.mock(Sandbox.class);
        when(sandbox.getId()).thenReturn(id);

        Sandbox.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
        when(sandbox.getResourceLimits()).thenReturn(Collections.singletonList(resourceLimitMock));

        return sandbox;
    }

    public static Sandbox.ResourceLimit createResourceLimitMock(String resourceTypeStr, Long threshold) {
        Sandbox.ResourceLimit resourceLimitMock = mock(Sandbox.ResourceLimit.class);
        SandboxResourceType resourceType = SandboxResourceType.fromString(resourceTypeStr);
        when(resourceLimitMock.getResourceType()).thenReturn(resourceType);
        when(resourceLimitMock.getThreshold()).thenReturn(threshold);
        when(resourceLimitMock.getThresholdInLong()).thenReturn(threshold);
        return resourceLimitMock;
    }

    public static SandboxLevelResourceUsageView createResourceUsageViewMock(String resourceTypeStr, Long usage) {
        SandboxLevelResourceUsageView mockView = mock(SandboxLevelResourceUsageView.class);
        SandboxResourceType resourceType = SandboxResourceType.fromString(resourceTypeStr);
        when(mockView.getResourceUsageData()).thenReturn(Collections.singletonMap(resourceType, usage));
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(1234), getRandomSearchTask(4321)));
        return mockView;
    }
}
