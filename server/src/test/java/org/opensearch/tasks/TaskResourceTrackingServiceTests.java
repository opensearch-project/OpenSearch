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
import java.util.concurrent.atomic.AtomicReference;

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

    private void verifyThreadContextFixedHeaders(String key, String value) {
        assertEquals(threadPool.getThreadContext().getHeader(key), value);
        assertEquals(threadPool.getThreadContext().getTransient(key), value);
        assertEquals(threadPool.getThreadContext().getResponseHeaders().get(key).get(0), value);
    }

}
