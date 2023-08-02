/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks;

import com.sun.management.ThreadMXBean;
import org.apache.lucene.util.Constants;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.NotifyOnceListener;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancelledException;
import org.opensearch.tasks.TaskId;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.tasks.TaskResourceUsage;
import org.opensearch.test.tasks.MockTaskManager;
import org.opensearch.test.tasks.MockTaskManagerListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.opensearch.tasks.TaskResourceTrackingService.TASK_ID;

@SuppressForbidden(reason = "ThreadMXBean#getThreadAllocatedBytes")
public class ResourceAwareTasksTests extends TaskManagerTestCase {

    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    public static class ResourceAwareNodeRequest extends TransportRequest {
        protected String requestName;

        public ResourceAwareNodeRequest() {
            super();
        }

        public ResourceAwareNodeRequest(StreamInput in) throws IOException {
            super(in);
            requestName = in.readString();
        }

        public ResourceAwareNodeRequest(NodesRequest request) {
            requestName = request.requestName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
        }

        @Override
        public String getDescription() {
            return "ResourceAwareNodeRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return false;
                }

                @Override
                public boolean supportsResourceTracking() {
                    return true;
                }
            };
        }
    }

    public static class NodesRequest extends BaseNodesRequest<NodesRequest> {
        private final String requestName;

        private NodesRequest(StreamInput in) throws IOException {
            super(in);
            requestName = in.readString();
        }

        public NodesRequest(String requestName, String... nodesIds) {
            super(nodesIds);
            this.requestName = requestName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
        }

        @Override
        public String getDescription() {
            return "NodesRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return true;
                }
            };
        }
    }

    /**
     * Simulates a task which executes work on search executor.
     */
    class ResourceAwareNodesAction extends AbstractTestNodesAction<NodesRequest, ResourceAwareNodeRequest> {
        private final TaskTestContext taskTestContext;
        private final boolean blockForCancellation;

        ResourceAwareNodesAction(
            String actionName,
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            boolean shouldBlock,
            TaskTestContext taskTestContext
        ) {
            super(actionName, threadPool, clusterService, transportService, NodesRequest::new, ResourceAwareNodeRequest::new);
            this.taskTestContext = taskTestContext;
            this.blockForCancellation = shouldBlock;
        }

        @Override
        protected ResourceAwareNodeRequest newNodeRequest(NodesRequest request) {
            return new ResourceAwareNodeRequest(request);
        }

        @Override
        protected NodeResponse nodeOperation(ResourceAwareNodeRequest request, Task task) {
            assert task.supportsResourceTracking();

            AtomicLong threadId = new AtomicLong();
            Future<?> result = threadPool.executor(ThreadPool.Names.SEARCH).submit(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    ExceptionsHelper.reThrowIfNotNull(e);
                }

                @Override
                @SuppressForbidden(reason = "ThreadMXBean#getThreadAllocatedBytes")
                protected void doRun() {
                    taskTestContext.memoryConsumptionWhenExecutionStarts = threadMXBean.getThreadAllocatedBytes(
                        Thread.currentThread().getId()
                    );
                    threadId.set(Thread.currentThread().getId());

                    // operationStartValidator will be called just before the task execution.
                    if (taskTestContext.operationStartValidator != null) {
                        taskTestContext.operationStartValidator.accept(task, threadId.get());
                    }

                    // operationFinishedValidator will be called just after all task threads are marked inactive and
                    // the task is unregistered.
                    if (taskTestContext.operationFinishedValidator != null) {
                        boolean success = task.addResourceTrackingCompletionListener(new NotifyOnceListener<>() {
                            @Override
                            protected void innerOnResponse(Task task) {
                                taskTestContext.operationFinishedValidator.accept(task, threadId.get());
                            }

                            @Override
                            protected void innerOnFailure(Exception e) {
                                ExceptionsHelper.reThrowIfNotNull(e);
                            }
                        });

                        if (success == false) {
                            fail("failed to register a completion listener as task resource tracking has already completed");
                        }
                    }

                    Object[] allocation1 = new Object[1000000]; // 4MB

                    if (blockForCancellation) {
                        // Simulate a job that takes forever to finish
                        // Using periodic checks method to identify that the task was cancelled
                        try {
                            boolean taskCancelled = waitUntil(((CancellableTask) task)::isCancelled);
                            if (taskCancelled) {
                                throw new TaskCancelledException("Task Cancelled");
                            } else {
                                fail("It should have thrown an exception");
                            }
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }

                    }

                    Object[] allocation2 = new Object[1000000]; // 4MB
                }
            });

            try {
                result.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }

            return new NodeResponse(clusterService.localNode());
        }

        @Override
        protected NodeResponse nodeOperation(ResourceAwareNodeRequest request) {
            throw new UnsupportedOperationException("the task parameter is required");
        }
    }

    private TaskTestContext startResourceAwareNodesAction(
        TestNode node,
        boolean blockForCancellation,
        TaskTestContext taskTestContext,
        ActionListener<NodesResponse> listener
    ) {
        NodesRequest request = new NodesRequest("Test Request", node.getNodeId());

        taskTestContext.requestCompleteLatch = new CountDownLatch(1);

        ResourceAwareNodesAction action = new ResourceAwareNodesAction(
            "internal:resourceAction",
            threadPool,
            node.clusterService,
            node.transportService,
            blockForCancellation,
            taskTestContext
        );
        taskTestContext.mainTask = action.execute(request, listener);
        return taskTestContext;
    }

    private static class TaskTestContext {
        private Task mainTask;
        private CountDownLatch requestCompleteLatch;
        private BiConsumer<Task, Long> operationStartValidator;
        private BiConsumer<Task, Long> operationFinishedValidator;
        private long memoryConsumptionWhenExecutionStarts;
    }

    public void testBasicTaskResourceTracking() throws Exception {
        setup(true, false);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        Map<Long, Task> resourceTasks = testNodes[0].taskResourceTrackingService.getResourceAwareTasks();

        taskTestContext.operationStartValidator = (task, threadId) -> {
            // One thread is currently working on task but not finished
            assertEquals(1, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertTrue(task.getResourceStats().get(threadId).get(0).isActive());
            assertEquals(0, task.getTotalResourceStats().getCpuTimeInNanos());
            assertEquals(0, task.getTotalResourceStats().getMemoryInBytes());
            assertEquals(0, task.getAverageResourceStats().getMemoryInBytes());
            assertEquals(0, task.getAverageResourceStats().getCpuTimeInNanos());
            assertEquals(0, task.getMinResourceStats().getMemoryInBytes());
            assertEquals(0, task.getMinResourceStats().getCpuTimeInNanos());
            assertEquals(0, task.getMaxResourceStats().getMemoryInBytes());
            assertEquals(0, task.getMaxResourceStats().getCpuTimeInNanos());
            assertEquals(1, task.getThreadUsage().getThreadExecutions());
            assertEquals(1, task.getThreadUsage().getActiveThreads());
        };

        taskTestContext.operationFinishedValidator = (task, threadId) -> {
            // Thread has finished working on the task's runnable
            assertEquals(0, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertFalse(task.getResourceStats().get(threadId).get(0).isActive());
            assertEquals(1, task.getThreadUsage().getThreadExecutions());
            assertEquals(0, task.getThreadUsage().getActiveThreads());

            long expectedArrayAllocationOverhead = 2 * 4000000; // Task's memory overhead due to array allocations
            long actualTaskMemoryOverhead = task.getTotalResourceStats().getMemoryInBytes();

            assertMemoryUsageWithinLimits(
                actualTaskMemoryOverhead - taskTestContext.memoryConsumptionWhenExecutionStarts,
                expectedArrayAllocationOverhead
            );
            assertCPUTime(task.getTotalResourceStats().getCpuTimeInNanos());
            // In basic single threaded case min == max == average == total
            assertEquals(task.getTotalResourceStats().getCpuTimeInNanos(), task.getAverageResourceStats().getCpuTimeInNanos());
            assertEquals(task.getTotalResourceStats().getCpuTimeInNanos(), task.getMinResourceStats().getCpuTimeInNanos());
            assertEquals(task.getTotalResourceStats().getCpuTimeInNanos(), task.getMaxResourceStats().getCpuTimeInNanos());
            assertEquals(task.getTotalResourceStats().getMemoryInBytes(), task.getAverageResourceStats().getMemoryInBytes());
            assertEquals(task.getTotalResourceStats().getMemoryInBytes(), task.getMinResourceStats().getMemoryInBytes());
            assertEquals(task.getTotalResourceStats().getMemoryInBytes(), task.getMaxResourceStats().getMemoryInBytes());
        };

        startResourceAwareNodesAction(testNodes[0], false, taskTestContext, new ActionListener<NodesResponse>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                responseReference.set(listTasksResponse);
                taskTestContext.requestCompleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throwableReference.set(e);
                taskTestContext.requestCompleteLatch.countDown();
            }
        });

        // Waiting for whole request to complete and return successfully till client
        taskTestContext.requestCompleteLatch.await();

        assertTasksRequestFinishedSuccessfully(responseReference.get(), throwableReference.get());
    }

    public void testTaskResourceTrackingDuringTaskCancellation() throws Exception {
        setup(true, false);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        Map<Long, Task> resourceTasks = testNodes[0].taskResourceTrackingService.getResourceAwareTasks();

        taskTestContext.operationStartValidator = (task, threadId) -> {
            // One thread is currently working on task but not finished
            assertEquals(1, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertTrue(task.getResourceStats().get(threadId).get(0).isActive());
            assertEquals(0, task.getTotalResourceStats().getCpuTimeInNanos());
            assertEquals(0, task.getTotalResourceStats().getMemoryInBytes());
        };

        taskTestContext.operationFinishedValidator = (task, threadId) -> {
            // Thread has finished working on the task's runnable
            assertEquals(0, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertFalse(task.getResourceStats().get(threadId).get(0).isActive());

            // allocations are completed before the task is cancelled
            long expectedArrayAllocationOverhead = 4000000; // Task's memory overhead due to array allocations
            long taskCancellationOverhead = 30000; // Task cancellation overhead ~ 30Kb
            long actualTaskMemoryOverhead = task.getTotalResourceStats().getMemoryInBytes();

            long expectedOverhead = expectedArrayAllocationOverhead + taskCancellationOverhead;
            assertMemoryUsageWithinLimits(
                actualTaskMemoryOverhead - taskTestContext.memoryConsumptionWhenExecutionStarts,
                expectedOverhead
            );
            assertCPUTime(task.getTotalResourceStats().getCpuTimeInNanos());
        };

        startResourceAwareNodesAction(testNodes[0], true, taskTestContext, new ActionListener<NodesResponse>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                responseReference.set(listTasksResponse);
                taskTestContext.requestCompleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throwableReference.set(e);
                taskTestContext.requestCompleteLatch.countDown();
            }
        });

        // Cancel main task
        CancelTasksRequest request = new CancelTasksRequest();
        request.setReason("Cancelling request to verify Task resource tracking behaviour");
        request.setTaskId(new TaskId(testNodes[0].getNodeId(), taskTestContext.mainTask.getId()));
        ActionTestUtils.executeBlocking(testNodes[0].transportCancelTasksAction, request);

        // Waiting for whole request to complete and return successfully till client
        taskTestContext.requestCompleteLatch.await();

        assertEquals(0, resourceTasks.size());
        assertNull(throwableReference.get());
        assertNotNull(responseReference.get());
        assertEquals(1, responseReference.get().failureCount());
        assertEquals(TaskCancelledException.class, findActualException(responseReference.get().failures().get(0)).getClass());
    }

    public void testTaskResourceTrackingDisabled() throws Exception {
        setup(false, false);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        Map<Long, Task> resourceTasks = testNodes[0].taskResourceTrackingService.getResourceAwareTasks();

        taskTestContext.operationStartValidator = (task, threadId) -> { assertEquals(0, resourceTasks.size()); };

        taskTestContext.operationFinishedValidator = (task, threadId) -> { assertEquals(0, resourceTasks.size()); };

        startResourceAwareNodesAction(testNodes[0], false, taskTestContext, new ActionListener<NodesResponse>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                responseReference.set(listTasksResponse);
                taskTestContext.requestCompleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throwableReference.set(e);
                taskTestContext.requestCompleteLatch.countDown();
            }
        });

        // Waiting for whole request to complete and return successfully till client
        taskTestContext.requestCompleteLatch.await();

        assertTasksRequestFinishedSuccessfully(responseReference.get(), throwableReference.get());
    }

    public void testTaskResourceTrackingDisabledWhileTaskInProgress() throws Exception {
        setup(true, false);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        Map<Long, Task> resourceTasks = testNodes[0].taskResourceTrackingService.getResourceAwareTasks();

        taskTestContext.operationStartValidator = (task, threadId) -> {
            // One thread is currently working on task but not finished
            assertEquals(1, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertTrue(task.getResourceStats().get(threadId).get(0).isActive());
            assertEquals(0, task.getTotalResourceStats().getCpuTimeInNanos());
            assertEquals(0, task.getTotalResourceStats().getMemoryInBytes());

            testNodes[0].taskResourceTrackingService.setTaskResourceTrackingEnabled(false);
        };

        taskTestContext.operationFinishedValidator = (task, threadId) -> {
            // Thread has finished working on the task's runnable
            assertEquals(0, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertFalse(task.getResourceStats().get(threadId).get(0).isActive());

            long expectedArrayAllocationOverhead = 2 * 4000000; // Task's memory overhead due to array allocations
            long actualTaskMemoryOverhead = task.getTotalResourceStats().getMemoryInBytes();

            assertMemoryUsageWithinLimits(
                actualTaskMemoryOverhead - taskTestContext.memoryConsumptionWhenExecutionStarts,
                expectedArrayAllocationOverhead
            );
            assertCPUTime(task.getTotalResourceStats().getCpuTimeInNanos());
        };

        startResourceAwareNodesAction(testNodes[0], false, taskTestContext, new ActionListener<NodesResponse>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                responseReference.set(listTasksResponse);
                taskTestContext.requestCompleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throwableReference.set(e);
                taskTestContext.requestCompleteLatch.countDown();
            }
        });

        // Waiting for whole request to complete and return successfully till client
        taskTestContext.requestCompleteLatch.await();

        assertTasksRequestFinishedSuccessfully(responseReference.get(), throwableReference.get());
    }

    public void testTaskResourceTrackingEnabledWhileTaskInProgress() throws Exception {
        setup(false, false);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        Map<Long, Task> resourceTasks = testNodes[0].taskResourceTrackingService.getResourceAwareTasks();

        taskTestContext.operationStartValidator = (task, threadId) -> {
            assertEquals(0, resourceTasks.size());

            testNodes[0].taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        };

        taskTestContext.operationFinishedValidator = (task, threadId) -> { assertEquals(0, resourceTasks.size()); };

        startResourceAwareNodesAction(testNodes[0], false, taskTestContext, new ActionListener<NodesResponse>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                responseReference.set(listTasksResponse);
                taskTestContext.requestCompleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throwableReference.set(e);
                taskTestContext.requestCompleteLatch.countDown();
            }
        });

        // Waiting for whole request to complete and return successfully till client
        taskTestContext.requestCompleteLatch.await();

        assertTasksRequestFinishedSuccessfully(responseReference.get(), throwableReference.get());
    }

    public void testOnDemandRefreshWhileFetchingTasks() throws InterruptedException {
        setup(true, false);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();

        TaskTestContext taskTestContext = new TaskTestContext();

        Map<Long, Task> resourceTasks = testNodes[0].taskResourceTrackingService.getResourceAwareTasks();

        taskTestContext.operationStartValidator = (task, threadId) -> {
            ListTasksResponse listTasksResponse = ActionTestUtils.executeBlocking(
                testNodes[0].transportListTasksAction,
                new ListTasksRequest().setActions("internal:resourceAction*").setDetailed(true)
            );

            TaskInfo taskInfo = listTasksResponse.getTasks().get(1);

            assertNotNull(taskInfo.getResourceStats());
            assertNotNull(taskInfo.getResourceStats().getResourceUsageInfo());
            assertTrue(taskInfo.getResourceStats().getResourceUsageInfo().get("total") instanceof TaskResourceUsage);
            TaskResourceUsage taskResourceUsage = (TaskResourceUsage) taskInfo.getResourceStats().getResourceUsageInfo().get("total");
            assertCPUTime(taskResourceUsage.getCpuTimeInNanos());
            assertTrue(taskResourceUsage.getMemoryInBytes() > 0);
        };

        taskTestContext.operationFinishedValidator = (task, threadId) -> { assertEquals(0, resourceTasks.size()); };

        startResourceAwareNodesAction(testNodes[0], false, taskTestContext, new ActionListener<NodesResponse>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                responseReference.set(listTasksResponse);
                taskTestContext.requestCompleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throwableReference.set(e);
                taskTestContext.requestCompleteLatch.countDown();
            }
        });

        // Waiting for whole request to complete and return successfully till client
        taskTestContext.requestCompleteLatch.await();

        assertTasksRequestFinishedSuccessfully(responseReference.get(), throwableReference.get());
    }

    public void testTaskIdPersistsInThreadContext() throws InterruptedException {
        setup(true, true);

        final List<Long> taskIdsAddedToThreadContext = new ArrayList<>();
        final List<Long> taskIdsRemovedFromThreadContext = new ArrayList<>();
        AtomicLong actualTaskIdInThreadContext = new AtomicLong(-1);
        AtomicLong expectedTaskIdInThreadContext = new AtomicLong(-2);

        ((MockTaskManager) testNodes[0].transportService.getTaskManager()).addListener(new MockTaskManagerListener() {
            @Override
            public void waitForTaskCompletion(Task task) {}

            @Override
            public void taskExecutionStarted(Task task, Boolean closeableInvoked) {
                if (closeableInvoked) {
                    taskIdsRemovedFromThreadContext.add(task.getId());
                } else {
                    taskIdsAddedToThreadContext.add(task.getId());
                }
            }

            @Override
            public void onTaskRegistered(Task task) {}

            @Override
            public void onTaskUnregistered(Task task) {
                if (task.getAction().equals("internal:resourceAction[n]")) {
                    expectedTaskIdInThreadContext.set(task.getId());
                    actualTaskIdInThreadContext.set(threadPool.getThreadContext().getTransient(TASK_ID));
                }
            }
        });

        TaskTestContext taskTestContext = new TaskTestContext();
        startResourceAwareNodesAction(testNodes[0], false, taskTestContext, new ActionListener<NodesResponse>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                taskTestContext.requestCompleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                taskTestContext.requestCompleteLatch.countDown();
            }
        });

        taskTestContext.requestCompleteLatch.await();

        // It is possible for the MockTaskManagerListener to be called after the response is sent already.
        // Wait enough time for taskId to be added to taskIdsRemovedFromThreadContext before performing validations.
        waitUntil(() -> taskIdsAddedToThreadContext.size() == taskIdsRemovedFromThreadContext.size(), 5, TimeUnit.SECONDS);

        assertEquals(expectedTaskIdInThreadContext.get(), actualTaskIdInThreadContext.get());
        assertThat(taskIdsAddedToThreadContext, containsInAnyOrder(taskIdsRemovedFromThreadContext.toArray()));
    }

    private void setup(boolean resourceTrackingEnabled, boolean useMockTaskManager) {
        Settings settings = Settings.builder()
            .put("task_resource_tracking.enabled", resourceTrackingEnabled)
            .put(MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.getKey(), useMockTaskManager)
            .build();
        setupTestNodes(settings);
        connectNodes(testNodes[0]);

        runnableTaskListener.set(testNodes[0].taskResourceTrackingService);
    }

    private Throwable findActualException(Exception e) {
        Throwable throwable = e.getCause();
        while (throwable.getCause() != null) {
            throwable = throwable.getCause();
        }
        return throwable;
    }

    private void assertTasksRequestFinishedSuccessfully(NodesResponse nodesResponse, Throwable throwable) {
        assertNull(throwable);
        assertNotNull(nodesResponse);
        assertEquals(0, nodesResponse.failureCount());
    }

    private void assertMemoryUsageWithinLimits(long actual, long expected) {
        // 5% buffer up to 500 KB to account for classloading overhead.
        long maxOverhead = Math.min(500000, expected * 5 / 100);
        assertThat(actual, lessThanOrEqualTo(expected + maxOverhead));
    }

    private void assertCPUTime(long cpuTimeInNanos) {
        // Windows registers a cpu tick at a default of ~15ms which is slightly slower than other OSs.
        // The work done within the runnable in this test often completes in under that time and returns a 0 value from
        // ThreadMXBean.getThreadCpuTime. To reduce flakiness in this test accept 0 as a value on Windows.
        if (Constants.WINDOWS) {
            assertTrue("Cpu should be non negative on windows", cpuTimeInNanos >= 0);
        } else {
            assertTrue("Cpu should have a positive value", cpuTimeInNanos > 0);
        }
    }
}
