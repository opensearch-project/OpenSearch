/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.opensearch.common.Priority;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class TaskBatcherTests extends TaskExecutorTests {

    protected TestTaskBatcher taskBatcher;

    @Before
    public void setUpBatchingTaskExecutor() throws Exception {
        taskBatcher = new TestTaskBatcher(logger, threadExecutor);
    }

    static class TestTaskBatcher extends TaskBatcher {

        TestTaskBatcher(Logger logger, PrioritizedOpenSearchThreadPoolExecutor threadExecutor) {
            super(logger, threadExecutor, getMockListener());
        }

        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, Function<Boolean, String> taskSummaryGenerator) {
            List<UpdateTask> updateTasks = (List) tasks;
            ((TestExecutor) batchingKey).execute(updateTasks.stream().map(t -> t.task).collect(Collectors.toList()));
            updateTasks.forEach(updateTask -> updateTask.listener.processed(updateTask.source));
        }

        @Override
        protected void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout) {
            threadPool.generic()
                .execute(
                    () -> tasks.forEach(
                        task -> ((UpdateTask) task).listener.onFailure(
                            task.source,
                            new ProcessClusterEventTimeoutException(timeout, task.source)
                        )
                    )
                );
        }

        class UpdateTask extends BatchedTask {
            final TestListener listener;

            UpdateTask(Priority priority, String source, Object task, TestListener listener, TestExecutor<?> executor) {
                super(priority, source, executor, task);
                this.listener = listener;
            }

            @Override
            public String describeTasks(List<? extends BatchedTask> tasks) {
                return ((TestExecutor<Object>) batchingKey).describeTasks(
                    tasks.stream().map(BatchedTask::getTask).collect(Collectors.toList())
                );
            }
        }

    }

    @Override
    protected void submitTask(String source, TestTask testTask) {
        submitTask(source, testTask, testTask, testTask, testTask);
    }

    private <T> void submitTask(String source, T task, ClusterStateTaskConfig config, TestExecutor<T> executor, TestListener listener) {
        submitTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    private <T> void submitTasks(
        final String source,
        final Map<T, TestListener> tasks,
        final ClusterStateTaskConfig config,
        final TestExecutor<T> executor
    ) {
        List<TestTaskBatcher.UpdateTask> safeTasks = tasks.entrySet()
            .stream()
            .map(e -> taskBatcher.new UpdateTask(config.priority(), source, e.getKey(), e.getValue(), executor))
            .collect(Collectors.toList());
        taskBatcher.submitTasks(safeTasks, config.timeout());
    }

    @Override
    public void testTimedOutTaskCleanedUp() throws Exception {
        super.testTimedOutTaskCleanedUp();
        synchronized (taskBatcher.tasksPerBatchingKey) {
            assertTrue("expected empty map but was " + taskBatcher.tasksPerBatchingKey, taskBatcher.tasksPerBatchingKey.isEmpty());
        }
    }

    public void testOneExecutorDoesntStarveAnother() throws InterruptedException {
        final List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
        final Semaphore allowProcessing = new Semaphore(0);
        final Semaphore startedProcessing = new Semaphore(0);

        class TaskExecutor implements TestExecutor<String> {

            @Override
            public void execute(List<String> tasks) {
                executionOrder.addAll(tasks); // do this first, so startedProcessing can be used as a notification that this is done.
                startedProcessing.release(tasks.size());
                try {
                    allowProcessing.acquire(tasks.size());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        TaskExecutor executorA = new TaskExecutor();
        TaskExecutor executorB = new TaskExecutor();

        final ClusterStateTaskConfig config = ClusterStateTaskConfig.build(Priority.NORMAL);
        final TestListener noopListener = (source, e) -> { throw new AssertionError(e); };
        // this blocks the cluster state queue, so we can set it up right
        submitTask("0", "A0", config, executorA, noopListener);
        // wait to be processed
        startedProcessing.acquire(1);
        assertThat(executionOrder, equalTo(Arrays.asList("A0")));

        // these will be the first batch
        submitTask("1", "A1", config, executorA, noopListener);
        submitTask("2", "A2", config, executorA, noopListener);

        // release the first 0 task, but not the second
        allowProcessing.release(1);
        startedProcessing.acquire(2);
        assertThat(executionOrder, equalTo(Arrays.asList("A0", "A1", "A2")));

        // setup the queue with pending tasks for another executor same priority
        submitTask("3", "B3", config, executorB, noopListener);
        submitTask("4", "B4", config, executorB, noopListener);

        submitTask("5", "A5", config, executorA, noopListener);
        submitTask("6", "A6", config, executorA, noopListener);

        // now release the processing
        allowProcessing.release(6);

        // wait for last task to be processed
        startedProcessing.acquire(4);

        assertThat(executionOrder, equalTo(Arrays.asList("A0", "A1", "A2", "B3", "B4", "A5", "A6")));
    }

    static class TaskExecutor implements TestExecutor<Integer> {
        List<Integer> tasks = new ArrayList<>();

        @Override
        public void execute(List<Integer> tasks) {
            this.tasks.addAll(tasks);
        }
    }

    // test that for a single thread, tasks are executed in the order
    // that they are submitted
    public void testTasksAreExecutedInOrder() throws BrokenBarrierException, InterruptedException {
        int numberOfThreads = randomIntBetween(2, 8);
        TaskExecutor[] executors = new TaskExecutor[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            executors[i] = new TaskExecutor();
        }

        // it will create at most 8192 threads, which will cause native memory oom. so we limit the number of created threads.
        int tasksSubmittedPerThread = randomIntBetween(2, 128);

        CopyOnWriteArrayList<Tuple<String, Throwable>> failures = new CopyOnWriteArrayList<>();
        CountDownLatch updateLatch = new CountDownLatch(numberOfThreads * tasksSubmittedPerThread);

        final TestListener listener = new TestListener() {
            @Override
            public void onFailure(String source, Exception e) {
                logger.error(() -> new ParameterizedMessage("unexpected failure: [{}]", source), e);
                failures.add(new Tuple<>(source, e));
                updateLatch.countDown();
            }

            @Override
            public void processed(String source) {
                updateLatch.countDown();
            }
        };

        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            final int index = i;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int j = 0; j < tasksSubmittedPerThread; j++) {
                        submitTask(
                            "[" + index + "][" + j + "]",
                            j,
                            ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                            executors[index],
                            listener
                        );
                    }
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
        }

        // wait for all threads to be ready
        barrier.await();
        // wait for all threads to finish
        barrier.await();

        updateLatch.await();

        assertThat(failures, empty());

        for (int i = 0; i < numberOfThreads; i++) {
            assertEquals(tasksSubmittedPerThread, executors[i].tasks.size());
            for (int j = 0; j < tasksSubmittedPerThread; j++) {
                assertNotNull(executors[i].tasks.get(j));
                assertEquals("cluster state update task executed out of order", j, (int) executors[i].tasks.get(j));
            }
        }
    }

    public void testNoTasksAreDroppedInParallelSubmission() throws BrokenBarrierException, InterruptedException {
        int numberOfThreads = randomIntBetween(2, 8);
        TaskExecutor[] executors = new TaskExecutor[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            executors[i] = new TaskExecutor();
        }

        int tasksSubmittedPerThread = randomIntBetween(2, 128);

        CopyOnWriteArrayList<Tuple<String, Throwable>> failures = new CopyOnWriteArrayList<>();
        CountDownLatch updateLatch = new CountDownLatch(numberOfThreads * tasksSubmittedPerThread);

        final TestListener listener = new TestListener() {
            @Override
            public void onFailure(String source, Exception e) {
                logger.error(() -> new ParameterizedMessage("unexpected failure: [{}]", source), e);
                failures.add(new Tuple<>(source, e));
                updateLatch.countDown();
            }

            @Override
            public void processed(String source) {
                updateLatch.countDown();
            }
        };

        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            final int index = i;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    CyclicBarrier tasksBarrier = new CyclicBarrier(1 + tasksSubmittedPerThread);
                    for (int j = 0; j < tasksSubmittedPerThread; j++) {
                        int taskNumber = j;
                        Thread taskThread = new Thread(() -> {
                            try {
                                tasksBarrier.await();
                                submitTask(
                                    "[" + index + "][" + taskNumber + "]",
                                    taskNumber,
                                    ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                                    executors[index],
                                    listener
                                );
                                tasksBarrier.await();
                            } catch (InterruptedException | BrokenBarrierException e) {
                                throw new AssertionError(e);
                            }
                        });
                        // submit tasks per batchingKey in parallel
                        taskThread.start();
                    }
                    // wait for all task threads to be ready
                    tasksBarrier.await();
                    // wait for all task threads to finish
                    tasksBarrier.await();
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
        }

        // wait for all executor threads to be ready
        barrier.await();
        // wait for all executor threads to finish
        barrier.await();

        updateLatch.await();

        assertThat(failures, empty());

        for (int i = 0; i < numberOfThreads; i++) {
            // assert that total executed tasks is same for every executor as we initiated
            assertEquals(tasksSubmittedPerThread, executors[i].tasks.size());
        }
    }

    public void testSingleBatchSubmission() throws InterruptedException {
        Map<Integer, TestListener> tasks = new HashMap<>();
        final int numOfTasks = randomInt(10);
        final CountDownLatch latch = new CountDownLatch(numOfTasks);
        Set<Integer> usedKeys = new HashSet<>(numOfTasks);
        for (int i = 0; i < numOfTasks; i++) {
            int key = randomValueOtherThanMany(k -> usedKeys.contains(k), () -> randomInt(1024));
            tasks.put(key, new TestListener() {
                @Override
                public void processed(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError(e);
                }
            });
            usedKeys.add(key);
        }
        assert usedKeys.size() == numOfTasks;

        TestExecutor<Integer> executor = taskList -> {
            assertThat(taskList.size(), equalTo(tasks.size()));
            assertThat(taskList.stream().collect(Collectors.toSet()), equalTo(tasks.keySet()));
        };
        submitTasks("test", tasks, ClusterStateTaskConfig.build(Priority.LANGUID), executor);

        latch.await();
    }

    public void testDuplicateSubmission() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        try (BlockingTask blockingTask = new BlockingTask(Priority.IMMEDIATE)) {
            submitTask("blocking", blockingTask);

            TestExecutor<SimpleTask> executor = tasks -> {};
            SimpleTask task1 = new SimpleTask(1);
            TestListener listener = new TestListener() {
                @Override
                public void processed(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError(e);
                }
            };

            submitTask("first time", task1, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);

            // submitting same task1 again, it should throw exception.
            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> submitTask("second time", task1, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener)
            );
            assertThat(e, hasToString(containsString("task [1] with source [second time] is already queued")));

            // inserting new task with same data, this should pass as it is new object and reference is different.
            SimpleTask task2 = new SimpleTask(1);
            // equals method returns true for both task
            assertTrue(task1.equals(task2));
            // references of both tasks are different.
            assertFalse(task1 == task2);
            // submitting this task should be allowed, as it is new object.
            submitTask("third time a charm", task2, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);

            // submitting same task2 again, it should throw exception, since it was submitted last time
            final IllegalStateException e2 = expectThrows(
                IllegalStateException.class,
                () -> submitTask("second time", task2, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener)
            );
            assertThat(e2, hasToString(containsString("task [1] with source [second time] is already queued")));

            assertThat(latch.getCount(), equalTo(2L));
        }
        latch.await();
    }

    public void testDuplicateSubmissionAfterTimeout() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch timeOutLatch = new CountDownLatch(1);
        try (BlockingTask blockingTask = new BlockingTask(Priority.IMMEDIATE)) {
            submitTask("blocking", blockingTask);

            TestExecutor<SimpleTask> executor = tasks -> {};
            SimpleTask task1 = new SimpleTask(1);
            TestListener listener = new TestListener() {
                @Override
                public void processed(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    if (e instanceof ProcessClusterEventTimeoutException) {
                        timeOutLatch.countDown();
                    } else {
                        throw new AssertionError(e);
                    }
                }
            };

            submitTask("first time", task1, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);
            ArrayList<TaskBatcher.BatchedTask> tasks = new ArrayList();
            tasks.add(
                taskBatcher.new UpdateTask(
                    ClusterStateTaskConfig.build(Priority.NORMAL).priority(), "first time", task1, listener, executor
                )
            );

            // task1 got timed out, it will be removed from map.
            taskBatcher.onTimeoutInternal(tasks, TimeValue.ZERO);
            timeOutLatch.await(); // wait for task to get timeout
            // submitting same task1 again, it should get submitted, since last task was timeout.
            submitTask("first time", task1, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);
            assertThat(latch.getCount(), equalTo(2L));
        }
        latch.await();
    }

    public void testDuplicateSubmissionAfterExecution() throws InterruptedException {
        final CountDownLatch firstTaskLatch = new CountDownLatch(1);
        final CountDownLatch latch = new CountDownLatch(2);

        TestExecutor<SimpleTask> executor = tasks -> {};
        SimpleTask task1 = new SimpleTask(1);
        TestListener listener = new TestListener() {
            @Override
            public void processed(String source) {
                firstTaskLatch.countDown();
                latch.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                if (e instanceof ProcessClusterEventTimeoutException) {
                    latch.countDown();
                } else {
                    throw new AssertionError(e);
                }
            }
        };
        submitTask("first time", task1, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);

        firstTaskLatch.await(); // wait till task is not executed

        // submitting same task1 again, it should get submitted, since last task was executed.
        submitTask("first time", task1, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);

        latch.await(); // wait till all tasks are not completed.
    }

    protected static TaskBatcherListener getMockListener() {
        return new TaskBatcherListener() {
            @Override
            public void onBeginSubmit(List<? extends TaskBatcher.BatchedTask> tasks) {
                // No Op
            }

            @Override
            public void onSubmitFailure(List<? extends TaskBatcher.BatchedTask> tasks) {
                // No Op
            }

            @Override
            public void onBeginProcessing(List<? extends TaskBatcher.BatchedTask> tasks) {
                // No Op
            }

            @Override
            public void onTimeout(List<? extends TaskBatcher.BatchedTask> tasks) {
                // No Op
            }
        };
    }

    private static class SimpleTask {
        private final int id;

        private SimpleTask(int id) {
            this.id = id;
        }

        @Override
        public int hashCode() {
            return this.id;
        }

        @Override
        public boolean equals(Object obj) {
            return ((SimpleTask) obj).getId() == this.id;
        }

        public int getId() {
            return id;
        }

        @Override
        public String toString() {
            return Integer.toString(id);
        }
    }

}
