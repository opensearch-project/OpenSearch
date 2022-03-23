/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancelledException;
import org.opensearch.tasks.TaskId;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.TaskAwareRunnable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class ResourceAwareTasksTests extends TaskManagerTestCase {

    // For every task there's a general overhead before and after the actual task operation code is executed.
    // This includes things like creating threadContext, Transport Channel, Tracking task cancellation etc.
    // For the tasks used for this test that maximum memory overhead can be 450Kb
    private static final int TASK_MAX_GENERAL_MEMORY_OVERHEAD = 450000;

    public static class ResourceAwareNodeRequest extends BaseNodeRequest {
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
                protected void doRun() {
                    threadId.set(Thread.currentThread().getId());

                    if (taskTestContext.operationStartValidator != null) {
                        try {
                            taskTestContext.operationStartValidator.accept(threadId.get());
                        } catch (AssertionError error) {
                            throw new RuntimeException(error);
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
            if (taskTestContext.operationFinishedValidator != null) {
                taskTestContext.operationFinishedValidator.accept(threadId.get());
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
        private Consumer<Long> operationStartValidator;
        private Consumer<Long> operationFinishedValidator;
    }

    public void testBasicTaskResourceTracking() throws Exception {
        setup(true);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        Map<Long, Task> resourceTasks = testNodes[0].transportService.getTaskManager().getResourceAwareTasks();

        taskTestContext.operationStartValidator = threadId -> {
            Task task = resourceTasks.values().stream().findAny().get();

            // One thread is currently working on task but not finished
            assertEquals(1, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertTrue(task.getResourceStats().get(threadId).get(0).isActive());
            assertEquals(0, task.getTotalResourceStats().getCpuTimeInNanos());
            assertEquals(0, task.getTotalResourceStats().getMemoryInBytes());
        };

        taskTestContext.operationFinishedValidator = threadId -> {
            Task task = resourceTasks.values().stream().findAny().get();

            // Thread has finished working on the task's runnable
            assertEquals(1, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertFalse(task.getResourceStats().get(threadId).get(0).isActive());

            long expectedArrayAllocationOverhead = 2 * 4012688; // Task's memory overhead due to array allocations
            long actualTaskMemoryOverhead = task.getTotalResourceStats().getMemoryInBytes();
            assertTrue(Math.abs(actualTaskMemoryOverhead - expectedArrayAllocationOverhead) < TASK_MAX_GENERAL_MEMORY_OVERHEAD);
            assertTrue(task.getTotalResourceStats().getCpuTimeInNanos() > 0);
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

        assertTasksRequestFinishedSuccessfully(resourceTasks.size(), responseReference.get(), throwableReference.get());
    }

    public void testTaskResourceTrackingDuringTaskCancellation() throws Exception {
        setup(true);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        Map<Long, Task> resourceTasks = testNodes[0].transportService.getTaskManager().getResourceAwareTasks();

        taskTestContext.operationStartValidator = threadId -> {
            Task task = resourceTasks.values().stream().findAny().get();

            // One thread is currently working on task but not finished
            assertEquals(1, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertTrue(task.getResourceStats().get(threadId).get(0).isActive());
            assertEquals(0, task.getTotalResourceStats().getCpuTimeInNanos());
            assertEquals(0, task.getTotalResourceStats().getMemoryInBytes());
        };

        taskTestContext.operationFinishedValidator = threadId -> {
            Task task = resourceTasks.values().stream().findAny().get();

            // Thread has finished working on the task's runnable
            assertEquals(1, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertFalse(task.getResourceStats().get(threadId).get(0).isActive());

            long expectedArrayAllocationOverhead = 4012688; // Task's memory overhead due to array allocations. Only one out of 2
            // allocations are completed before the task is cancelled
            long actualArrayAllocationOverhead = task.getTotalResourceStats().getMemoryInBytes();

            assertTrue(Math.abs(actualArrayAllocationOverhead - expectedArrayAllocationOverhead) < TASK_MAX_GENERAL_MEMORY_OVERHEAD);
            assertTrue(task.getTotalResourceStats().getCpuTimeInNanos() > 0);
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
        setup(false);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        Map<Long, Task> resourceTasks = testNodes[0].transportService.getTaskManager().getResourceAwareTasks();

        taskTestContext.operationStartValidator = threadId -> { assertEquals(0, resourceTasks.size()); };

        taskTestContext.operationFinishedValidator = threadId -> { assertEquals(0, resourceTasks.size()); };

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

        assertTasksRequestFinishedSuccessfully(resourceTasks.size(), responseReference.get(), throwableReference.get());
    }

    public void testTaskResourceTrackingDisabledWhileTaskInProgress() throws Exception {
        setup(true);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        TaskManager taskManager = testNodes[0].transportService.getTaskManager();
        Map<Long, Task> resourceTasks = taskManager.getResourceAwareTasks();

        taskTestContext.operationStartValidator = threadId -> {
            Task task = resourceTasks.values().stream().findAny().get();
            // One thread is currently working on task but not finished
            assertEquals(1, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertTrue(task.getResourceStats().get(threadId).get(0).isActive());
            assertEquals(0, task.getTotalResourceStats().getCpuTimeInNanos());
            assertEquals(0, task.getTotalResourceStats().getMemoryInBytes());

            taskManager.setTaskResourceTrackingEnabled(false);
        };

        taskTestContext.operationFinishedValidator = threadId -> {
            Task task = resourceTasks.values().stream().findAny().get();
            // Thread has finished working on the task's runnable
            assertEquals(1, resourceTasks.size());
            assertEquals(1, task.getResourceStats().size());
            assertEquals(1, task.getResourceStats().get(threadId).size());
            assertFalse(task.getResourceStats().get(threadId).get(0).isActive());

            long expectedArrayAllocationOverhead = 2 * 4012688; // Task's memory overhead due to array allocations
            long actualTaskMemoryOverhead = task.getTotalResourceStats().getMemoryInBytes();
            assertTrue(Math.abs(actualTaskMemoryOverhead - expectedArrayAllocationOverhead) < TASK_MAX_GENERAL_MEMORY_OVERHEAD);
            assertTrue(task.getTotalResourceStats().getCpuTimeInNanos() > 0);
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

        assertTasksRequestFinishedSuccessfully(resourceTasks.size(), responseReference.get(), throwableReference.get());
    }

    public void testTaskResourceTrackingEnabledWhileTaskInProgress() throws Exception {
        setup(false);

        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        TaskTestContext taskTestContext = new TaskTestContext();

        TaskManager taskManager = testNodes[0].transportService.getTaskManager();
        Map<Long, Task> resourceTasks = taskManager.getResourceAwareTasks();

        taskTestContext.operationStartValidator = threadId -> {
            assertEquals(0, resourceTasks.size());

            taskManager.setTaskResourceTrackingEnabled(true);
        };

        taskTestContext.operationFinishedValidator = threadId -> { assertEquals(0, resourceTasks.size()); };

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

        assertTasksRequestFinishedSuccessfully(resourceTasks.size(), responseReference.get(), throwableReference.get());
    }

    private void setup(boolean resourceTrackingEnabled) {
        Settings settings = Settings.builder().put("task_resource_tracking.enabled", resourceTrackingEnabled).build();
        setupTestNodes(settings);
        connectNodes(testNodes[0]);
        TaskAwareRunnable.setListener(testNodes[0].transportService.getTaskManager());
    }

    private Throwable findActualException(Exception e) {
        Throwable throwable = e.getCause();
        while (throwable.getCause() != null) {
            throwable = throwable.getCause();
        }
        return throwable;
    }

    private void assertTasksRequestFinishedSuccessfully(int activeResourceTasks, NodesResponse nodesResponse, Throwable throwable) {
        assertEquals(0, activeResourceTasks);
        assertNull(throwable);
        assertNotNull(nodesResponse);
        assertEquals(0, nodesResponse.failureCount());
    }

}
