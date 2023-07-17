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
import org.opensearch.Version;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.tasks.TaskCancellationMonitoringSettings.DURATION_MILLIS_SETTING;

public class TaskCancellationMonitoringServiceTests extends OpenSearchTestCase {

    MockTransportService transportService;
    TaskManager taskManager;
    ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
        taskManager = transportService.getTaskManager();
        taskManager.setTaskCancellationService(new TaskCancellationService(transportService));
    }

    @After
    public void cleanup() {
        transportService.close();
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testWithNoCurrentRunningCancelledTasks() {
        TaskCancellationMonitoringSettings settings = new TaskCancellationMonitoringSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        TaskManager mockTaskManager = mock(TaskManager.class);
        TaskCancellationMonitoringService taskCancellationMonitoringService = new TaskCancellationMonitoringService(
            threadPool,
            mockTaskManager,
            settings
        );

        taskCancellationMonitoringService.doRun();
        // Task manager should not be invoked.
        verify(mockTaskManager, times(0)).getTasks();
    }

    public void testWithNonZeroCancelledSearchShardTasksRunning() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(DURATION_MILLIS_SETTING.getKey(), 0) // Setting to zero for testing
            .build();
        TaskCancellationMonitoringSettings taskCancellationMonitoringSettings = new TaskCancellationMonitoringSettings(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        TaskCancellationMonitoringService taskCancellationMonitoringService = new TaskCancellationMonitoringService(
            threadPool,
            taskManager,
            taskCancellationMonitoringSettings
        );
        int numTasks = randomIntBetween(5, 50);
        List<SearchShardTask> tasks = createTasks(numTasks);

        int cancelFromIdx = randomIntBetween(0, numTasks - 1);
        int cancelTillIdx = randomIntBetween(cancelFromIdx, numTasks - 1);

        int numberOfTasksCancelled = cancelTillIdx - cancelFromIdx + 1;
        CountDownLatch countDownLatch = cancelTasksConcurrently(tasks, cancelFromIdx, cancelTillIdx);

        countDownLatch.await(); // Wait for all threads execution.
        taskCancellationMonitoringService.doRun(); // 1st run to verify whether we are able to track running cancelled
        // tasks.
        TaskCancellationStats stats = taskCancellationMonitoringService.stats();
        assertEquals(numberOfTasksCancelled, stats.getSearchShardTaskCancellationStats().getCurrentLongRunningCancelledTaskCount());
        assertEquals(numberOfTasksCancelled, stats.getSearchShardTaskCancellationStats().getTotalLongRunningCancelledTaskCount());

        taskCancellationMonitoringService.doRun(); // 2nd run. Verify same.
        stats = taskCancellationMonitoringService.stats();
        assertEquals(numberOfTasksCancelled, stats.getSearchShardTaskCancellationStats().getCurrentLongRunningCancelledTaskCount());
        assertEquals(numberOfTasksCancelled, stats.getSearchShardTaskCancellationStats().getTotalLongRunningCancelledTaskCount());
        completeTasksConcurrently(tasks, 0, tasks.size() - 1).await();
        taskCancellationMonitoringService.doRun(); // 3rd run to verify current count is 0 and total remains the same.
        stats = taskCancellationMonitoringService.stats();
        assertTrue(taskCancellationMonitoringService.getCancelledTaskTracker().isEmpty());
        assertEquals(0, stats.getSearchShardTaskCancellationStats().getCurrentLongRunningCancelledTaskCount());
        assertEquals(numberOfTasksCancelled, stats.getSearchShardTaskCancellationStats().getTotalLongRunningCancelledTaskCount());
    }

    public void testShouldRunGetsDisabledAfterTaskCompletion() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(DURATION_MILLIS_SETTING.getKey(), 0) // Setting to zero for testing
            .build();
        TaskCancellationMonitoringSettings taskCancellationMonitoringSettings = new TaskCancellationMonitoringSettings(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        TaskCancellationMonitoringService taskCancellationMonitoringService = new TaskCancellationMonitoringService(
            threadPool,
            taskManager,
            taskCancellationMonitoringSettings
        );
        assertTrue(taskCancellationMonitoringService.getCancelledTaskTracker().isEmpty());
        assertEquals(0, taskCancellationMonitoringService.getCancelledTaskTracker().size());

        // Start few tasks.
        int numTasks = randomIntBetween(5, 50);
        List<SearchShardTask> tasks = createTasks(numTasks);

        taskCancellationMonitoringService.doRun();
        TaskCancellationStats stats = taskCancellationMonitoringService.stats();
        // verify no cancelled tasks currently being recorded
        assertEquals(0, stats.getSearchShardTaskCancellationStats().getCurrentLongRunningCancelledTaskCount());
        assertEquals(0, stats.getSearchShardTaskCancellationStats().getTotalLongRunningCancelledTaskCount());
        cancelTasksConcurrently(tasks, 0, tasks.size() - 1).await();
        taskCancellationMonitoringService.doRun();
        stats = taskCancellationMonitoringService.stats();
        assertFalse(taskCancellationMonitoringService.getCancelledTaskTracker().isEmpty());
        assertEquals(numTasks, stats.getSearchShardTaskCancellationStats().getCurrentLongRunningCancelledTaskCount());
        assertEquals(numTasks, stats.getSearchShardTaskCancellationStats().getTotalLongRunningCancelledTaskCount());

        completeTasksConcurrently(tasks, 0, tasks.size() - 1).await();
        stats = taskCancellationMonitoringService.stats();
        assertTrue(taskCancellationMonitoringService.getCancelledTaskTracker().isEmpty());
        assertEquals(0, stats.getSearchShardTaskCancellationStats().getCurrentLongRunningCancelledTaskCount());
        assertEquals(numTasks, stats.getSearchShardTaskCancellationStats().getTotalLongRunningCancelledTaskCount());
    }

    public void testWithVaryingCancelledTasksDuration() throws InterruptedException {
        long cancelledTaskDurationThresholdMilis = 2000;
        Settings settings = Settings.builder()
            .put(DURATION_MILLIS_SETTING.getKey(), cancelledTaskDurationThresholdMilis) // Setting to one for testing
            .build();
        TaskCancellationMonitoringSettings taskCancellationMonitoringSettings = new TaskCancellationMonitoringSettings(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        TaskCancellationMonitoringService taskCancellationMonitoringService = new TaskCancellationMonitoringService(
            threadPool,
            taskManager,
            taskCancellationMonitoringSettings
        );

        int numTasks = randomIntBetween(5, 50);
        List<SearchShardTask> tasks = createTasks(numTasks);

        int numTasksToBeCancelledInFirstIteration = randomIntBetween(1, numTasks - 1);
        CountDownLatch countDownLatch = cancelTasksConcurrently(tasks, 0, numTasksToBeCancelledInFirstIteration - 1);
        countDownLatch.await(); // Wait for all tasks to be cancelled in first iteration

        Thread.sleep(cancelledTaskDurationThresholdMilis); // Sleep, so we later verify whether above tasks are being
        // captured as part of stats.

        taskCancellationMonitoringService.doRun();
        TaskCancellationStats stats = taskCancellationMonitoringService.stats();
        // Verify only tasks that were cancelled as part of first iteration is being captured as part of stats as
        // they have been running longer as per threshold.
        assertEquals(
            numTasksToBeCancelledInFirstIteration,
            stats.getSearchShardTaskCancellationStats().getCurrentLongRunningCancelledTaskCount()
        );
        assertEquals(
            numTasksToBeCancelledInFirstIteration,
            stats.getSearchShardTaskCancellationStats().getTotalLongRunningCancelledTaskCount()
        );

        countDownLatch = cancelTasksConcurrently(tasks, numTasksToBeCancelledInFirstIteration, numTasks - 1);
        countDownLatch.await(); // Wait for rest of tasks to be cancelled.

        Thread.sleep(cancelledTaskDurationThresholdMilis); // Sleep again, so we now verify whether all tasks are
        // being captured as part of stats.
        taskCancellationMonitoringService.doRun();
        stats = taskCancellationMonitoringService.stats();
        assertEquals(numTasks, stats.getSearchShardTaskCancellationStats().getCurrentLongRunningCancelledTaskCount());
        assertEquals(numTasks, stats.getSearchShardTaskCancellationStats().getTotalLongRunningCancelledTaskCount());

        completeTasksConcurrently(tasks, 0, tasks.size() - 1).await();
        taskCancellationMonitoringService.doRun();
        stats = taskCancellationMonitoringService.stats();
        // Verify no current running tasks
        assertEquals(0, stats.getSearchShardTaskCancellationStats().getCurrentLongRunningCancelledTaskCount());
        assertEquals(numTasks, stats.getSearchShardTaskCancellationStats().getTotalLongRunningCancelledTaskCount());
    }

    public void testTasksAreGettingEvictedCorrectlyAfterCompletion() throws InterruptedException {
        Settings settings = Settings.builder()
            .put(DURATION_MILLIS_SETTING.getKey(), 0) // Setting to one for testing
            .build();
        TaskCancellationMonitoringSettings taskCancellationMonitoringSettings = new TaskCancellationMonitoringSettings(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        TaskCancellationMonitoringService taskCancellationMonitoringService = new TaskCancellationMonitoringService(
            threadPool,
            taskManager,
            taskCancellationMonitoringSettings
        );

        // Start few tasks.
        int numTasks = randomIntBetween(5, 50);
        List<SearchShardTask> tasks = createTasks(numTasks);
        assertTrue(taskCancellationMonitoringService.getCancelledTaskTracker().isEmpty());
        int numTasksToBeCancelledInFirstIteration = randomIntBetween(2, numTasks - 1);
        CountDownLatch countDownLatch = cancelTasksConcurrently(tasks, 0, numTasksToBeCancelledInFirstIteration - 1);
        countDownLatch.await(); // Wait for all tasks to be cancelled in first iteration

        assertEquals(numTasksToBeCancelledInFirstIteration, taskCancellationMonitoringService.getCancelledTaskTracker().size());
        // Verify desired task ids are present.
        for (int itr = 0; itr < numTasksToBeCancelledInFirstIteration; itr++) {
            assertTrue(taskCancellationMonitoringService.getCancelledTaskTracker().containsKey(tasks.get(itr).getId()));
        }
        // Cancel rest of the tasks
        cancelTasksConcurrently(tasks, numTasksToBeCancelledInFirstIteration, numTasks - 1).await();
        for (int itr = 0; itr < tasks.size(); itr++) {
            assertTrue(taskCancellationMonitoringService.getCancelledTaskTracker().containsKey(tasks.get(itr).getId()));
        }
        // Complete one task to start with.
        completeTasksConcurrently(tasks, 0, 0).await();
        assertFalse(taskCancellationMonitoringService.getCancelledTaskTracker().containsKey(tasks.get(0).getId()));
        // Verify rest of the tasks are still present in tracker
        for (int itr = 1; itr < tasks.size(); itr++) {
            assertTrue(taskCancellationMonitoringService.getCancelledTaskTracker().containsKey(tasks.get(itr).getId()));
        }
        // Complete first iteration tasks
        completeTasksConcurrently(tasks, 1, numTasksToBeCancelledInFirstIteration - 1).await();
        // Verify desired tasks were evicted from tracker map
        for (int itr = 0; itr < numTasksToBeCancelledInFirstIteration; itr++) {
            assertFalse(taskCancellationMonitoringService.getCancelledTaskTracker().containsKey(tasks.get(0).getId()));
        }
        // Verify rest of the tasks are still present in tracker
        for (int itr = numTasksToBeCancelledInFirstIteration; itr < tasks.size(); itr++) {
            assertTrue(taskCancellationMonitoringService.getCancelledTaskTracker().containsKey(tasks.get(itr).getId()));
        }
        // Complete all of them finally
        completeTasksConcurrently(tasks, numTasksToBeCancelledInFirstIteration, tasks.size() - 1).await();
        assertTrue(taskCancellationMonitoringService.getCancelledTaskTracker().isEmpty());
        for (int itr = 0; itr < tasks.size(); itr++) {
            assertFalse(taskCancellationMonitoringService.getCancelledTaskTracker().containsKey(tasks.get(itr).getId()));
        }
    }

    public void testDoStartAndStop() {
        TaskCancellationMonitoringSettings settings = new TaskCancellationMonitoringSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        Scheduler.Cancellable scheduleFuture = mock(Scheduler.Cancellable.class);
        when(scheduleFuture.cancel()).thenReturn(true);
        when(mockThreadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(scheduleFuture);
        TaskCancellationMonitoringService taskCancellationMonitoringService = new TaskCancellationMonitoringService(
            mockThreadPool,
            taskManager,
            settings
        );

        taskCancellationMonitoringService.doStart();
        taskCancellationMonitoringService.doStop();
        verify(scheduleFuture, times(1)).cancel();
    }

    private List<SearchShardTask> createTasks(int numTasks) {
        List<SearchShardTask> tasks = new ArrayList<>(numTasks);
        for (int i = 0; i < numTasks; i++) {
            tasks.add((SearchShardTask) taskManager.register("type-" + i, "action-" + i, new MockQuerySearchRequest()));
        }
        return tasks;
    }

    // Caller can this with the list of tasks specifically mentioning which ones to cancel. And can call CountDownLatch
    // .await() to wait for all tasks be cancelled.
    private CountDownLatch cancelTasksConcurrently(List<? extends CancellableTask> tasks, int cancelFromIdx, int cancelTillIdx) {
        assert cancelFromIdx >= 0;
        assert cancelTillIdx <= tasks.size() - 1;
        assert cancelTillIdx >= cancelFromIdx;
        int totalTasksToBeCancelled = cancelTillIdx - cancelFromIdx + 1;
        Thread[] threads = new Thread[totalTasksToBeCancelled];
        Phaser phaser = new Phaser(totalTasksToBeCancelled + 1); // Used to concurrently cancel tasks by multiple threads.
        CountDownLatch countDownLatch = new CountDownLatch(totalTasksToBeCancelled); // To wait for all threads to finish.
        for (int i = 0; i < totalTasksToBeCancelled; i++) {
            int idx = i + cancelFromIdx;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                taskManager.cancel(tasks.get(idx), "test", () -> {});
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        return countDownLatch;
    }

    private CountDownLatch completeTasksConcurrently(List<? extends CancellableTask> tasks, int completeFromIdx, int completeTillIdx) {
        assert completeFromIdx >= 0;
        assert completeTillIdx <= tasks.size() - 1;
        assert completeTillIdx >= completeFromIdx;
        int totalTasksToBeCompleted = completeTillIdx - completeFromIdx + 1;
        Thread[] threads = new Thread[totalTasksToBeCompleted];
        Phaser phaser = new Phaser(totalTasksToBeCompleted + 1);
        CountDownLatch countDownLatch = new CountDownLatch(totalTasksToBeCompleted);
        for (int i = 0; i < totalTasksToBeCompleted; i++) {
            int idx = i + completeFromIdx;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                taskManager.unregister(tasks.get(idx));
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        return countDownLatch;
    }

    public static class MockQuerySearchRequest extends TransportRequest {
        protected String requestName;

        public MockQuerySearchRequest() {
            super();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
        }

        @Override
        public String getDescription() {
            return "MockQuerySearchRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }

}
