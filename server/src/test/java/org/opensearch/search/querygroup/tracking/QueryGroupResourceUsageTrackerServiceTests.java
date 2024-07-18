/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup.tracking;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.search.querygroup.QueryGroupLevelResourceUsageView;
import org.opensearch.search.querygroup.QueryGroupTask;
import org.opensearch.search.querygroup.tracker.QueryGroupResourceUsageTrackerService;
import org.opensearch.search.resourcetypes.ResourceType;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryGroupResourceUsageTrackerServiceTests extends OpenSearchTestCase {
    TestThreadPool threadPool;
    TaskManager taskManager;
    TaskResourceTrackingService mockTaskResourceTrackingService;
    QueryGroupResourceUsageTrackerService queryGroupResourceUsageTrackerService;

    @Before
    public void setup() {
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        threadPool = new TestThreadPool(getTestName());
        mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        queryGroupResourceUsageTrackerService = new QueryGroupResourceUsageTrackerService(taskManager, mockTaskResourceTrackingService);
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testConstructQueryGroupLevelViews_CreatesQueryGroupLevelUsageView_WhenTasksArePresent() {
        List<String> queryGroupIds = List.of("queryGroup1", "queryGroup2", "queryGroup3");

        Map<Long, Task> activeSearchShardTasks = createActiveSearchShardTasks(queryGroupIds);
        when(mockTaskResourceTrackingService.getResourceAwareTasks()).thenReturn(activeSearchShardTasks);
        Map<String, QueryGroupLevelResourceUsageView> stringQueryGroupLevelResourceUsageViewMap = queryGroupResourceUsageTrackerService
            .constructQueryGroupLevelUsageViews();

        for (String queryGroupId : queryGroupIds) {
            assertEquals(
                400,
                (long) stringQueryGroupLevelResourceUsageViewMap.get(queryGroupId).getResourceUsageData().get(ResourceType.fromName("JVM"))
            );
            assertEquals(2, stringQueryGroupLevelResourceUsageViewMap.get(queryGroupId).getActiveTasks().size());
        }
    }

    public void testConstructQueryGroupLevelViews_CreatesQueryGroupLevelUsageView_WhenTasksAreNotPresent() {
        Map<String, QueryGroupLevelResourceUsageView> stringQueryGroupLevelResourceUsageViewMap = queryGroupResourceUsageTrackerService
            .constructQueryGroupLevelUsageViews();
        assertTrue(stringQueryGroupLevelResourceUsageViewMap.isEmpty());
    }

    public void testConstructQueryGroupLevelUsageViews_WithTasksHavingDifferentResourceUsage() {
        Map<Long, Task> activeSearchShardTasks = new HashMap<>();
        activeSearchShardTasks.put(1L, createMockTask(SearchShardTask.class, 100, 200, "queryGroup1"));
        activeSearchShardTasks.put(2L, createMockTask(SearchShardTask.class, 200, 400, "queryGroup1"));
        when(mockTaskResourceTrackingService.getResourceAwareTasks()).thenReturn(activeSearchShardTasks);

        Map<String, QueryGroupLevelResourceUsageView> queryGroupViews = queryGroupResourceUsageTrackerService
            .constructQueryGroupLevelUsageViews();

        assertEquals(600, (long) queryGroupViews.get("queryGroup1").getResourceUsageData().get(ResourceType.fromName("JVM")));
        assertEquals(2, queryGroupViews.get("queryGroup1").getActiveTasks().size());
    }

    private Map<Long, Task> createActiveSearchShardTasks(List<String> queryGroupIds) {
        Map<Long, Task> activeSearchShardTasks = new HashMap<>();
        long task_id = 0;
        for (String queryGroupId : queryGroupIds) {
            for (int i = 0; i < 2; i++) {
                activeSearchShardTasks.put(++task_id, createMockTask(SearchShardTask.class, 100, 200, queryGroupId));
            }
        }
        return activeSearchShardTasks;
    }

    private <T extends CancellableTask> T createMockTask(Class<T> type, long cpuUsage, long heapUsage, String queryGroupId) {
        T task = mock(type);
        if (task instanceof SearchTask || task instanceof SearchShardTask) {
            when(((QueryGroupTask) task).getQueryGroupId()).thenReturn(queryGroupId);
        }
        when(task.getTotalResourceStats()).thenReturn(new TaskResourceUsage(cpuUsage, heapUsage));
        when(task.getStartTimeNanos()).thenReturn((long) 0);

        AtomicBoolean isCancelled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            isCancelled.set(true);
            return null;
        }).when(task).cancel(anyString());
        doAnswer(invocation -> isCancelled.get()).when(task).isCancelled();

        return task;
    }
}
