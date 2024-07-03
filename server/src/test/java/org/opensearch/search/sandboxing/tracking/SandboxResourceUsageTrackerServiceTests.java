/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.tracking;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.search.sandboxing.SandboxLevelResourceUsageView;
import org.opensearch.search.sandboxing.SandboxTask;
import org.opensearch.search.sandboxing.resourcetype.SystemResource;
import org.opensearch.search.sandboxing.tracker.SandboxResourceUsageTrackerService;
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

public class SandboxResourceUsageTrackerServiceTests extends OpenSearchTestCase {
    TestThreadPool threadPool;
    TaskManager taskManager;
    TaskResourceTrackingService mockTaskResourceTrackingService;
    SandboxResourceUsageTrackerService sandboxResourceUsageTrackerService;

    @Before
    public void setup() {
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        threadPool = new TestThreadPool(getTestName());
        mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        sandboxResourceUsageTrackerService = new SandboxResourceUsageTrackerService(taskManager, mockTaskResourceTrackingService);
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testConstructSandboxLevelViews_CreatesSandboxLevelUsageView_WhenTasksArePresent() {
        List<String> sandboxIds = List.of("sandbox1", "sandbox2", "sandbox3");

        Map<Long, Task> activeSearchShardTasks = createActiveSearchShardTasks(sandboxIds);
        when(mockTaskResourceTrackingService.getResourceAwareTasks()).thenReturn(activeSearchShardTasks);
        Map<String, SandboxLevelResourceUsageView> stringSandboxLevelResourceUsageViewMap = sandboxResourceUsageTrackerService
            .constructSandboxLevelUsageViews();

        for (String sandboxId : sandboxIds) {
            assertEquals(
                400,
                (long) stringSandboxLevelResourceUsageViewMap.get(sandboxId).getResourceUsageData().get(SystemResource.fromString("JVM"))
            );
            assertEquals(2, stringSandboxLevelResourceUsageViewMap.get(sandboxId).getActiveTasks().size());
        }
    }

    public void testConstructSandboxLevelViews_CreatesSandboxLevelUsageView_WhenTasksAreNotPresent() {
        Map<String, SandboxLevelResourceUsageView> stringSandboxLevelResourceUsageViewMap = sandboxResourceUsageTrackerService
            .constructSandboxLevelUsageViews();
        assertTrue(stringSandboxLevelResourceUsageViewMap.isEmpty());
    }

    public void testConstructSandboxLevelUsageViews_WithTasksHavingDifferentResourceUsage() {
        Map<Long, Task> activeSearchShardTasks = new HashMap<>();
        activeSearchShardTasks.put(1L, createMockTask(SearchShardTask.class, 100, 200, "sandbox1"));
        activeSearchShardTasks.put(2L, createMockTask(SearchShardTask.class, 200, 400, "sandbox1"));
        when(mockTaskResourceTrackingService.getResourceAwareTasks()).thenReturn(activeSearchShardTasks);

        Map<String, SandboxLevelResourceUsageView> sandboxViews = sandboxResourceUsageTrackerService.constructSandboxLevelUsageViews();

        assertEquals(600, (long) sandboxViews.get("sandbox1").getResourceUsageData().get(SystemResource.fromString("JVM")));
        assertEquals(2, sandboxViews.get("sandbox1").getActiveTasks().size());
    }

    private Map<Long, Task> createActiveSearchShardTasks(List<String> sandboxIds) {
        Map<Long, Task> activeSearchShardTasks = new HashMap<>();
        long task_id = 0;
        for (String sandboxId : sandboxIds) {
            for (int i = 0; i < 2; i++) {
                activeSearchShardTasks.put(++task_id, createMockTask(SearchShardTask.class, 100, 200, sandboxId));
            }
        }
        return activeSearchShardTasks;
    }

    private <T extends CancellableTask> T createMockTask(Class<T> type, long cpuUsage, long heapUsage, String sandboxId) {
        T task = mock(type);
        if (task instanceof SearchTask || task instanceof SearchShardTask) {
            when(((SandboxTask) task).getSandboxId()).thenReturn(sandboxId);
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
