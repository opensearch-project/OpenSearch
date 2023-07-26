/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.admin.cluster.node.tasks.TransportTasksActionTests;
import org.opensearch.action.search.SearchTask;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.tasks.ResourceStats.CPU;
import static org.opensearch.tasks.ResourceStats.MEMORY;
import static org.opensearch.tasks.TaskResourceTrackingService.TASK_ID;

public class TaskResourceTrackingServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private TaskResourceTrackingService taskResourceTrackingService;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(TransportTasksActionTests.class.getSimpleName(), new AtomicReference<>());
        taskResourceTrackingService = new TaskResourceTrackingService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testThreadContextUpdateOnTrackingStart() {
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);

        Task task = new SearchTask(1, "test", "test", () -> "Test", TaskId.EMPTY_TASK_ID, new HashMap<>());

        String key = "KEY";
        String value = "VALUE";

        // Prepare thread context
        threadPool.getThreadContext().putHeader(key, value);
        threadPool.getThreadContext().putTransient(key, value);
        threadPool.getThreadContext().addResponseHeader(key, value);

        ThreadContext.StoredContext storedContext = taskResourceTrackingService.startTracking(task);

        // All headers should be preserved and Task Id should also be included in thread context
        verifyThreadContextFixedHeaders(key, value);
        assertEquals((long) threadPool.getThreadContext().getTransient(TASK_ID), task.getId());

        storedContext.restore();

        // Post restore only task id should be removed from the thread context
        verifyThreadContextFixedHeaders(key, value);
        assertNull(threadPool.getThreadContext().getTransient(TASK_ID));
    }

    public void testStopTrackingHandlesCurrentActiveThread() {
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        Task task = new SearchTask(1, "test", "test", () -> "Test", TaskId.EMPTY_TASK_ID, new HashMap<>());
        ThreadContext.StoredContext storedContext = taskResourceTrackingService.startTracking(task);
        long threadId = Thread.currentThread().getId();
        taskResourceTrackingService.taskExecutionStartedOnThread(task.getId(), threadId);

        assertTrue(task.getResourceStats().get(threadId).get(0).isActive());
        assertEquals(0, task.getResourceStats().get(threadId).get(0).getResourceUsageInfo().getStatsInfo().get(MEMORY).getTotalValue());

        taskResourceTrackingService.stopTracking(task);

        // Makes sure stop tracking marks the current active thread inactive and refreshes the resource stats before returning.
        assertFalse(task.getResourceStats().get(threadId).get(0).isActive());
        assertTrue(task.getResourceStats().get(threadId).get(0).getResourceUsageInfo().getStatsInfo().get(MEMORY).getTotalValue() > 0);
    }

    /**
     * Test if taskResourceTrackingService properly tracks resource usage when multiple threads work on the same task
     */
    public void testStartingTrackingHandlesMultipleThreadsPerTask() throws InterruptedException {
        ExecutorService executor = threadPool.executor(ThreadPool.Names.GENERIC);
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        Task task = new SearchTask(1, "test", "test", () -> "Test", TaskId.EMPTY_TASK_ID, new HashMap<>());
        taskResourceTrackingService.startTracking(task);
        int numTasks = randomIntBetween(2, 100);
        for (int i = 0; i < numTasks; i++) {
            executor.execute(() -> {
                long threadId = Thread.currentThread().getId();
                taskResourceTrackingService.taskExecutionStartedOnThread(task.getId(), threadId);
                // The same thread may pick up multiple runnables for the same task id
                assertEquals(1, task.getResourceStats().get(threadId).stream().filter(ThreadResourceInfo::isActive).count());
                taskResourceTrackingService.taskExecutionFinishedOnThread(task.getId(), threadId);
            });
        }
        executor.shutdown();
        while (true) {
            try {
                if (executor.awaitTermination(1, TimeUnit.MINUTES)) break;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Map<Long, List<ThreadResourceInfo>> stats = task.getResourceStats();
        int numExecutions = 0;
        for (Long threadId : stats.keySet()) {
            for (ThreadResourceInfo info : task.getResourceStats().get(threadId)) {
                assertTrue(info.getResourceUsageInfo().getStatsInfo().get(MEMORY).getTotalValue() > 0);
                assertTrue(info.getResourceUsageInfo().getStatsInfo().get(CPU).getTotalValue() > 0);
                assertFalse(info.isActive());
                numExecutions++;
            }

        }
        assertTrue(task.getTotalResourceStats().getCpuTimeInNanos() > 0);
        assertTrue(task.getTotalResourceStats().getMemoryInBytes() > 0);
        // Basic sanity testing that min < average < max < total
        assertTrue(task.getMinResourceStats().getMemoryInBytes() < task.getAverageResourceStats().getMemoryInBytes());
        assertTrue(task.getAverageResourceStats().getMemoryInBytes() < task.getMaxResourceStats().getMemoryInBytes());
        assertTrue(task.getMaxResourceStats().getMemoryInBytes() < task.getTotalResourceStats().getMemoryInBytes());
        // Each execution of a runnable should record an entry in resourceStats even if it's the same thread
        assertEquals(numTasks, numExecutions);
    }

    private void verifyThreadContextFixedHeaders(String key, String value) {
        assertEquals(threadPool.getThreadContext().getHeader(key), value);
        assertEquals(threadPool.getThreadContext().getTransient(key), value);
        assertEquals(threadPool.getThreadContext().getResponseHeaders().get(key).get(0), value);
    }

}
