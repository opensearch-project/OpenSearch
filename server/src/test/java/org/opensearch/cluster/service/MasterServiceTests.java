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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.LocalClusterUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.BaseFuture;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class MasterServiceTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private static long relativeTimeInMillis;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(MasterServiceTests.class.getName()) {
            @Override
            public long relativeTimeInMillis() {
                return relativeTimeInMillis;
            }
        };
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void randomizeCurrentTime() {
        relativeTimeInMillis = randomLongBetween(0L, 1L << 62);
    }

    private ClusterManagerService createClusterManagerService(boolean makeClusterManager) {
        final DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final ClusterManagerService clusterManagerService = new ClusterManagerService(
            Settings.builder()
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
                .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        final ClusterState initialClusterState = ClusterState.builder(new ClusterName(MasterServiceTests.class.getSimpleName()))
            .nodes(
                DiscoveryNodes.builder()
                    .add(localNode)
                    .localNodeId(localNode.getId())
                    .clusterManagerNodeId(makeClusterManager ? localNode.getId() : null)
            )
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialClusterState);
        clusterManagerService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            clusterStateRef.set(event.state());
            publishListener.onResponse(null);
        });
        clusterManagerService.setClusterStateSupplier(clusterStateRef::get);
        clusterManagerService.start();
        return clusterManagerService;
    }

    public void testClusterManagerAwareExecution() throws Exception {
        final ClusterManagerService nonClusterManager = createClusterManagerService(false);

        final boolean[] taskFailed = { false };
        final CountDownLatch latch1 = new CountDownLatch(1);
        nonClusterManager.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                latch1.countDown();
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                taskFailed[0] = true;
                latch1.countDown();
            }
        });

        latch1.await();
        assertTrue("cluster state update task was executed on a non-cluster-manager", taskFailed[0]);

        final CountDownLatch latch2 = new CountDownLatch(1);
        nonClusterManager.submitStateUpdateTask("test", new LocalClusterUpdateTask() {
            @Override
            public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                taskFailed[0] = false;
                latch2.countDown();
                return unchanged();
            }

            @Override
            public void onFailure(String source, Exception e) {
                taskFailed[0] = true;
                latch2.countDown();
            }
        });
        latch2.await();
        assertFalse("non-cluster-manager cluster state update task was not executed", taskFailed[0]);

        nonClusterManager.close();
    }

    public void testThreadContext() throws InterruptedException {
        final ClusterManagerService clusterManagerService = createClusterManagerService(true);
        final CountDownLatch latch = new CountDownLatch(1);

        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            final Map<String, String> expectedHeaders = Collections.singletonMap("test", "test");
            final Map<String, List<String>> expectedResponseHeaders = Collections.singletonMap(
                "testResponse",
                Collections.singletonList("testResponse")
            );
            threadPool.getThreadContext().putHeader(expectedHeaders);

            final TimeValue ackTimeout = randomBoolean() ? TimeValue.ZERO : TimeValue.timeValueMillis(randomInt(10000));
            final TimeValue clusterManagerTimeout = randomBoolean() ? TimeValue.ZERO : TimeValue.timeValueMillis(randomInt(10000));

            clusterManagerService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask<Void>(null, null) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assertTrue(threadPool.getThreadContext().isSystemContext());
                    assertEquals(Collections.emptyMap(), threadPool.getThreadContext().getHeaders());
                    threadPool.getThreadContext().addResponseHeader("testResponse", "testResponse");
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());

                    if (randomBoolean()) {
                        return ClusterState.builder(currentState).build();
                    } else if (randomBoolean()) {
                        return currentState;
                    } else {
                        throw new IllegalArgumentException("mock failure");
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }

                @Override
                protected Void newResponse(boolean acknowledged) {
                    return null;
                }

                public TimeValue ackTimeout() {
                    return ackTimeout;
                }

                @Override
                public TimeValue timeout() {
                    return clusterManagerTimeout;
                }

                @Override
                public void onAllNodesAcked(@Nullable Exception e) {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }

                @Override
                public void onAckTimeout() {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }

            });

            assertFalse(threadPool.getThreadContext().isSystemContext());
            assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
            assertEquals(Collections.emptyMap(), threadPool.getThreadContext().getResponseHeaders());
        }

        latch.await();

        clusterManagerService.close();
    }

    /*
    * test that a listener throwing an exception while handling a
    * notification does not prevent publication notification to the
    * executor
    */
    public void testClusterStateTaskListenerThrowingExceptionIsOkay() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean published = new AtomicBoolean();

        try (ClusterManagerService clusterManagerService = createClusterManagerService(true)) {
            clusterManagerService.submitStateUpdateTask(
                "testClusterStateTaskListenerThrowingExceptionIsOkay",
                new Object(),
                ClusterStateTaskConfig.build(Priority.NORMAL),
                new ClusterStateTaskExecutor<Object>() {
                    @Override
                    public ClusterTasksResult<Object> execute(ClusterState currentState, List<Object> tasks) {
                        ClusterState newClusterState = ClusterState.builder(currentState).build();
                        return ClusterTasksResult.builder().successes(tasks).build(newClusterState);
                    }

                    @Override
                    public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                        published.set(true);
                        latch.countDown();
                    }
                },
                new ClusterStateTaskListener() {
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        throw new IllegalStateException(source);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {}
                }
            );

            latch.await();
            assertTrue(published.get());
        }
    }

    @TestLogging(value = "org.opensearch.cluster.service:TRACE", reason = "to ensure that we log cluster state events on TRACE level")
    public void testClusterStateUpdateLogging() throws Exception {
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(MasterService.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test1 start",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "executing cluster state update for [test1]"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test1 computation",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "took [1s] to compute cluster state update for [test1]"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test1 notification",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "took [0s] to notify listeners on unchanged cluster state for [test1]"
                )
            );

            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test2 start",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "executing cluster state update for [test2]"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test2 failure",
                    MasterService.class.getCanonicalName(),
                    Level.TRACE,
                    "failed to execute cluster state update (on version: [*], uuid: [*]) for [test2]*"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test2 computation",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "took [2s] to compute cluster state update for [test2]"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test2 notification",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "took [0s] to notify listeners on unchanged cluster state for [test2]"
                )
            );

            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test3 start",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "executing cluster state update for [test3]"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test3 computation",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "took [3s] to compute cluster state update for [test3]"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test3 notification",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "took [4s] to notify listeners on successful publication of cluster state (version: *, uuid: *) for [test3]"
                )
            );

            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test4",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "executing cluster state update for [test4]"
                )
            );

            try (ClusterManagerService clusterManagerService = createClusterManagerService(true)) {
                clusterManagerService.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        relativeTimeInMillis += TimeValue.timeValueSeconds(1).millis();
                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {}

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
                clusterManagerService.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        relativeTimeInMillis += TimeValue.timeValueSeconds(2).millis();
                        throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        fail();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {}
                });
                clusterManagerService.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        relativeTimeInMillis += TimeValue.timeValueSeconds(3).millis();
                        return ClusterState.builder(currentState).incrementVersion().build();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        relativeTimeInMillis += TimeValue.timeValueSeconds(4).millis();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
                clusterManagerService.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {}

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
                assertBusy(mockAppender::assertAllExpectationsMatched);
            }
        }
    }

    public void testClusterStateBatchedUpdates() throws BrokenBarrierException, InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        class Task {
            private AtomicBoolean state = new AtomicBoolean();
            private final int id;

            Task(int id) {
                this.id = id;
            }

            public void execute() {
                if (!state.compareAndSet(false, true)) {
                    throw new IllegalStateException();
                } else {
                    counter.incrementAndGet();
                }
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Task task = (Task) o;
                return id == task.id;

            }

            @Override
            public int hashCode() {
                return id;
            }

            @Override
            public String toString() {
                return Integer.toString(id);
            }
        }

        int numberOfThreads = randomIntBetween(2, 8);
        int taskSubmissionsPerThread = randomIntBetween(1, 64);
        int numberOfExecutors = Math.max(1, numberOfThreads / 4);
        final Semaphore semaphore = new Semaphore(numberOfExecutors);

        class TaskExecutor implements ClusterStateTaskExecutor<Task> {
            private final List<Set<Task>> taskGroups;
            private AtomicInteger counter = new AtomicInteger();
            private AtomicInteger batches = new AtomicInteger();
            private AtomicInteger published = new AtomicInteger();

            TaskExecutor(List<Set<Task>> taskGroups) {
                this.taskGroups = taskGroups;
            }

            @Override
            public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
                for (Set<Task> expectedSet : taskGroups) {
                    long count = tasks.stream().filter(expectedSet::contains).count();
                    assertThat(
                        "batched set should be executed together or not at all. Expected " + expectedSet + "s. Executing " + tasks,
                        count,
                        anyOf(equalTo(0L), equalTo((long) expectedSet.size()))
                    );
                }
                tasks.forEach(Task::execute);
                counter.addAndGet(tasks.size());
                ClusterState maybeUpdatedClusterState = currentState;
                if (randomBoolean()) {
                    maybeUpdatedClusterState = ClusterState.builder(currentState).build();
                    batches.incrementAndGet();
                    semaphore.acquire();
                }
                return ClusterTasksResult.<Task>builder().successes(tasks).build(maybeUpdatedClusterState);
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                published.incrementAndGet();
                semaphore.release();
            }
        }

        ConcurrentMap<String, AtomicInteger> processedStates = new ConcurrentHashMap<>();

        List<Set<Task>> taskGroups = new ArrayList<>();
        List<TaskExecutor> executors = new ArrayList<>();
        for (int i = 0; i < numberOfExecutors; i++) {
            executors.add(new TaskExecutor(taskGroups));
        }

        // randomly assign tasks to executors
        List<Tuple<TaskExecutor, Set<Task>>> assignments = new ArrayList<>();
        int taskId = 0;
        for (int i = 0; i < numberOfThreads; i++) {
            for (int j = 0; j < taskSubmissionsPerThread; j++) {
                TaskExecutor executor = randomFrom(executors);
                Set<Task> tasks = new HashSet<>();
                for (int t = randomInt(3); t >= 0; t--) {
                    tasks.add(new Task(taskId++));
                }
                taskGroups.add(tasks);
                assignments.add(Tuple.tuple(executor, tasks));
            }
        }

        Map<TaskExecutor, Integer> counts = new HashMap<>();
        int totalTaskCount = 0;
        for (Tuple<TaskExecutor, Set<Task>> assignment : assignments) {
            final int taskCount = assignment.v2().size();
            counts.merge(assignment.v1(), taskCount, (previous, count) -> previous + count);
            totalTaskCount += taskCount;
        }
        final CountDownLatch updateLatch = new CountDownLatch(totalTaskCount);
        final ClusterStateTaskListener listener = new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                processedStates.computeIfAbsent(source, key -> new AtomicInteger()).incrementAndGet();
                updateLatch.countDown();
            }
        };

        try (ClusterManagerService clusterManagerService = createClusterManagerService(true)) {
            final ConcurrentMap<String, AtomicInteger> submittedTasksPerThread = new ConcurrentHashMap<>();
            CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
            for (int i = 0; i < numberOfThreads; i++) {
                final int index = i;
                Thread thread = new Thread(() -> {
                    final String threadName = Thread.currentThread().getName();
                    try {
                        barrier.await();
                        for (int j = 0; j < taskSubmissionsPerThread; j++) {
                            Tuple<TaskExecutor, Set<Task>> assignment = assignments.get(index * taskSubmissionsPerThread + j);
                            final Set<Task> tasks = assignment.v2();
                            submittedTasksPerThread.computeIfAbsent(threadName, key -> new AtomicInteger()).addAndGet(tasks.size());
                            final TaskExecutor executor = assignment.v1();
                            if (tasks.size() == 1) {
                                clusterManagerService.submitStateUpdateTask(
                                    threadName,
                                    tasks.stream().findFirst().get(),
                                    ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                                    executor,
                                    listener
                                );
                            } else {
                                Map<Task, ClusterStateTaskListener> taskListeners = new HashMap<>();
                                tasks.forEach(t -> taskListeners.put(t, listener));
                                clusterManagerService.submitStateUpdateTasks(
                                    threadName,
                                    taskListeners,
                                    ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                                    executor
                                );
                            }
                        }
                        barrier.await();
                    } catch (BrokenBarrierException | InterruptedException e) {
                        throw new AssertionError(e);
                    }
                });
                thread.start();
            }

            // wait for all threads to be ready
            barrier.await();
            // wait for all threads to finish
            barrier.await();

            // wait until all the cluster state updates have been processed
            updateLatch.await();
            // and until all of the publication callbacks have completed
            semaphore.acquire(numberOfExecutors);

            // assert the number of executed tasks is correct
            assertEquals(totalTaskCount, counter.get());

            // assert each executor executed the correct number of tasks
            for (TaskExecutor executor : executors) {
                if (counts.containsKey(executor)) {
                    assertEquals((int) counts.get(executor), executor.counter.get());
                    assertEquals(executor.batches.get(), executor.published.get());
                }
            }

            // assert the correct number of clusterStateProcessed events were triggered
            for (Map.Entry<String, AtomicInteger> entry : processedStates.entrySet()) {
                assertThat(submittedTasksPerThread, hasKey(entry.getKey()));
                assertEquals(
                    "not all tasks submitted by " + entry.getKey() + " received a processed event",
                    entry.getValue().get(),
                    submittedTasksPerThread.get(entry.getKey()).get()
                );
            }
        }
    }

    public void testThrottlingForTaskSubmission() throws InterruptedException {
        MasterService masterService = createClusterManagerService(true);
        int throttlingLimit = randomIntBetween(1, 10);
        int taskId = 1;
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch latch = new CountDownLatch(1);
        final String taskName = "test";
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey = masterService.registerClusterManagerTask(taskName, true);
        class Task {
            private final int id;

            Task(int id) {
                this.id = id;
            }
        }

        class TaskExecutor implements ClusterStateTaskExecutor<Task> {
            private AtomicInteger published = new AtomicInteger();

            @Override
            public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
                latch.countDown();
                barrier.await();
                return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
            }

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return throttlingKey;
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                published.incrementAndGet();
            }
        }

        masterService.clusterManagerTaskThrottler.updateLimit(taskName, throttlingLimit);

        final ClusterStateTaskListener listener = new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {}

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {}
        };

        TaskExecutor executor = new TaskExecutor();
        // submit one task which will be execution, post that will submit throttlingLimit tasks.
        try {
            masterService.submitStateUpdateTask(
                taskName,
                new Task(taskId++),
                ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                executor,
                listener
            );
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        // wait till task enter in execution.
        latch.await();

        for (int i = 0; i < throttlingLimit; i++) {
            try {
                masterService.submitStateUpdateTask(
                    taskName,
                    new Task(taskId++),
                    ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                    executor,
                    listener
                );
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        // we have one task in execution and tasks in queue so next task should throttled.
        final AtomicReference<ClusterManagerThrottlingException> assertionRef = new AtomicReference<>();
        try {
            masterService.submitStateUpdateTask(
                taskName,
                new Task(taskId++),
                ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                executor,
                listener
            );
        } catch (ClusterManagerThrottlingException e) {
            assertionRef.set(e);
        }
        assertNotNull(assertionRef.get());
        masterService.close();
    }

    public void testThrottlingForMultipleTaskTypes() throws InterruptedException {
        MasterService masterService = createClusterManagerService(true);
        int throttlingLimitForTask1 = randomIntBetween(1, 5);
        int throttlingLimitForTask2 = randomIntBetween(1, 5);
        int throttlingLimitForTask3 = randomIntBetween(1, 5);
        int numberOfTask1 = randomIntBetween(throttlingLimitForTask1, 10);
        int numberOfTask2 = randomIntBetween(throttlingLimitForTask2, 10);
        int numberOfTask3 = randomIntBetween(throttlingLimitForTask3, 10);
        String task1 = "Task1";
        String task2 = "Task2";
        String task3 = "Task3";

        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey1 = masterService.registerClusterManagerTask(task1, true);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey2 = masterService.registerClusterManagerTask(task2, true);
        ClusterManagerTaskThrottler.ThrottlingKey throttlingKey3 = masterService.registerClusterManagerTask(task3, true);
        class Task {}
        class Task1 extends Task {}
        class Task2 extends Task {}
        class Task3 extends Task {}

        class Task1Executor implements ClusterStateTaskExecutor<Task> {
            @Override
            public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
                Thread.sleep(randomInt(1000));
                return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {}

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return throttlingKey1;
            }
        }

        class Task2Executor implements ClusterStateTaskExecutor<Task> {
            @Override
            public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
                Thread.sleep(randomInt(1000));
                return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {}

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return throttlingKey2;
            }
        }

        class Task3Executor implements ClusterStateTaskExecutor<Task> {
            @Override
            public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
                Thread.sleep(randomInt(1000));
                return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {}

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return throttlingKey3;
            }
        }

        // configuring limits for Task1 and Task3. All task submission of Task2 should pass.
        masterService.clusterManagerTaskThrottler.updateLimit(task1, throttlingLimitForTask1);
        masterService.clusterManagerTaskThrottler.updateLimit(task3, throttlingLimitForTask3);
        final CountDownLatch latch = new CountDownLatch(numberOfTask1 + numberOfTask2 + numberOfTask3);
        AtomicInteger throttledTask1 = new AtomicInteger();
        AtomicInteger throttledTask2 = new AtomicInteger();
        AtomicInteger throttledTask3 = new AtomicInteger();
        AtomicInteger succeededTask1 = new AtomicInteger();
        AtomicInteger succeededTask2 = new AtomicInteger();
        AtomicInteger timedOutTask3 = new AtomicInteger();

        final ClusterStateTaskListener listener = new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {
                // Task3's timeout should have called this.
                assertEquals(task3, source);
                timedOutTask3.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (source.equals(task1)) {
                    succeededTask1.incrementAndGet();
                } else if (source.equals(task2)) {
                    succeededTask2.incrementAndGet();
                }
                latch.countDown();
            }
        };
        Task1Executor executor1 = new Task1Executor();
        Task2Executor executor2 = new Task2Executor();
        Task3Executor executor3 = new Task3Executor();
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < numberOfTask1; i++) {
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        masterService.submitStateUpdateTask(
                            task1,
                            new Task1(),
                            ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                            executor1,
                            listener
                        );
                    } catch (ClusterManagerThrottlingException e) {
                        // Exception should be RejactedExecutionException.
                        throttledTask1.incrementAndGet();
                        latch.countDown();
                    }
                }
            }));
        }
        for (int i = 0; i < numberOfTask2; i++) {
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        masterService.submitStateUpdateTask(
                            task2,
                            new Task2(),
                            ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                            executor2,
                            listener
                        );
                    } catch (ClusterManagerThrottlingException e) {
                        throttledTask2.incrementAndGet();
                        latch.countDown();
                    }
                }
            }));
        }
        for (int i = 0; i < numberOfTask3; i++) {
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        masterService.submitStateUpdateTask(
                            task3,
                            new Task3(),
                            ClusterStateTaskConfig.build(randomFrom(Priority.values()), new TimeValue(0)),
                            executor3,
                            listener
                        );
                    } catch (ClusterManagerThrottlingException e) {
                        throttledTask3.incrementAndGet();
                        latch.countDown();
                    }
                }
            }));
        }
        for (Thread thread : threads) {
            thread.start();
        }

        // await for latch to clear
        latch.await();
        assertEquals(numberOfTask1, throttledTask1.get() + succeededTask1.get());
        assertEquals(numberOfTask2, succeededTask2.get());
        assertEquals(0, throttledTask2.get());
        assertEquals(numberOfTask3, throttledTask3.get() + timedOutTask3.get());
        masterService.close();
    }

    public void testBlockingCallInClusterStateTaskListenerFails() throws InterruptedException {
        assumeTrue("assertions must be enabled for this test to work", BaseFuture.class.desiredAssertionStatus());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AssertionError> assertionRef = new AtomicReference<>();

        try (ClusterManagerService clusterManagerService = createClusterManagerService(true)) {
            clusterManagerService.submitStateUpdateTask(
                "testBlockingCallInClusterStateTaskListenerFails",
                new Object(),
                ClusterStateTaskConfig.build(Priority.NORMAL),
                (currentState, tasks) -> {
                    ClusterState newClusterState = ClusterState.builder(currentState).build();
                    return ClusterStateTaskExecutor.ClusterTasksResult.builder().successes(tasks).build(newClusterState);
                },
                new ClusterStateTaskListener() {
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        BaseFuture<Void> future = new BaseFuture<Void>() {
                        };
                        try {
                            if (randomBoolean()) {
                                future.get(1L, TimeUnit.SECONDS);
                            } else {
                                future.get();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } catch (AssertionError e) {
                            assertionRef.set(e);
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(String source, Exception e) {}
                }
            );

            latch.await();
            assertNotNull(assertionRef.get());
            assertThat(assertionRef.get().getMessage(), containsString("Reason: [Blocking operation]"));
        }
    }

    @TestLogging(value = "org.opensearch.cluster.service:WARN", reason = "to ensure that we log cluster state events on WARN level")
    public void testLongClusterStateUpdateLogging() throws Exception {
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(MasterService.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "test1 shouldn't log because it was fast enough",
                    MasterService.class.getCanonicalName(),
                    Level.WARN,
                    "*took*test1*"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test2",
                    MasterService.class.getCanonicalName(),
                    Level.WARN,
                    "*took [*], which is over [10s], to compute cluster state update for [test2]"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test3",
                    MasterService.class.getCanonicalName(),
                    Level.WARN,
                    "*took [*], which is over [10s], to compute cluster state update for [test3]"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test4",
                    MasterService.class.getCanonicalName(),
                    Level.WARN,
                    "*took [*], which is over [10s], to compute cluster state update for [test4]"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "test5 should not log despite publishing slowly",
                    MasterService.class.getCanonicalName(),
                    Level.WARN,
                    "*took*test5*"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test6 should log due to slow and failing publication",
                    MasterService.class.getCanonicalName(),
                    Level.WARN,
                    "took [*] and then failed to publish updated cluster state (version: *, uuid: *) for [test6]:*"
                )
            );

            try (
                ClusterManagerService clusterManagerService = new ClusterManagerService(
                    Settings.builder()
                        .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
                        .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
                        .build(),
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    threadPool
                )
            ) {

                final DiscoveryNode localNode = new DiscoveryNode(
                    "node1",
                    buildNewFakeTransportAddress(),
                    emptyMap(),
                    emptySet(),
                    Version.CURRENT
                );
                final ClusterState initialClusterState = ClusterState.builder(new ClusterName(MasterServiceTests.class.getSimpleName()))
                    .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
                    .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                    .build();
                final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialClusterState);
                clusterManagerService.setClusterStatePublisher((event, publishListener, ackListener) -> {
                    if (event.source().contains("test5")) {
                        relativeTimeInMillis += ClusterManagerService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(
                            Settings.EMPTY
                        ).millis() + randomLongBetween(1, 1000000);
                    }
                    if (event.source().contains("test6")) {
                        relativeTimeInMillis += ClusterManagerService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(
                            Settings.EMPTY
                        ).millis() + randomLongBetween(1, 1000000);
                        throw new OpenSearchException("simulated error during slow publication which should trigger logging");
                    }
                    clusterStateRef.set(event.state());
                    publishListener.onResponse(null);
                });
                clusterManagerService.setClusterStateSupplier(clusterStateRef::get);
                clusterManagerService.start();

                final CountDownLatch latch = new CountDownLatch(6);
                final CountDownLatch processedFirstTask = new CountDownLatch(1);
                clusterManagerService.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        relativeTimeInMillis += randomLongBetween(
                            0L,
                            ClusterManagerService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        );
                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        latch.countDown();
                        processedFirstTask.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });

                processedFirstTask.await();
                clusterManagerService.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        relativeTimeInMillis += ClusterManagerService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(
                            Settings.EMPTY
                        ).millis() + randomLongBetween(1, 1000000);
                        throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        fail();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        latch.countDown();
                    }
                });
                clusterManagerService.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        relativeTimeInMillis += ClusterManagerService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(
                            Settings.EMPTY
                        ).millis() + randomLongBetween(1, 1000000);
                        return ClusterState.builder(currentState).incrementVersion().build();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
                clusterManagerService.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        relativeTimeInMillis += ClusterManagerService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(
                            Settings.EMPTY
                        ).millis() + randomLongBetween(1, 1000000);
                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
                clusterManagerService.submitStateUpdateTask("test5", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState).incrementVersion().build();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
                clusterManagerService.submitStateUpdateTask("test6", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState).incrementVersion().build();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        fail();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail(); // maybe we should notify here?
                    }
                });
                // Additional update task to make sure all previous logging made it to the loggerName
                // We don't check logging for this on since there is no guarantee that it will occur before our check
                clusterManagerService.submitStateUpdateTask("test7", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
                latch.await();
            }
            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testAcking() throws InterruptedException {
        final DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final DiscoveryNode node3 = new DiscoveryNode("node3", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        try (
            ClusterManagerService clusterManagerService = new ClusterManagerService(
                Settings.builder()
                    .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
                    .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
                    .build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool
            )
        ) {

            final ClusterState initialClusterState = ClusterState.builder(new ClusterName(MasterServiceTests.class.getSimpleName()))
                .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3).localNodeId(node1.getId()).masterNodeId(node1.getId()))
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                .build();
            final AtomicReference<ClusterStatePublisher> publisherRef = new AtomicReference<>();
            clusterManagerService.setClusterStatePublisher((e, pl, al) -> publisherRef.get().publish(e, pl, al));
            clusterManagerService.setClusterStateSupplier(() -> initialClusterState);
            clusterManagerService.start();

            // check that we don't time out before even committing the cluster state
            {
                final CountDownLatch latch = new CountDownLatch(1);

                publisherRef.set(
                    (clusterChangedEvent, publishListener, ackListener) -> publishListener.onFailure(
                        new FailedToCommitClusterStateException("mock exception")
                    )
                );

                clusterManagerService.submitStateUpdateTask("test2", new AckedClusterStateUpdateTask<Void>(null, null) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState).build();
                    }

                    @Override
                    public TimeValue ackTimeout() {
                        return TimeValue.ZERO;
                    }

                    @Override
                    public TimeValue timeout() {
                        return null;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        fail();
                    }

                    @Override
                    protected Void newResponse(boolean acknowledged) {
                        fail();
                        return null;
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        latch.countDown();
                    }

                    @Override
                    public void onAckTimeout() {
                        fail();
                    }
                });

                latch.await();
            }

            // check that we timeout if commit took too long
            {
                final CountDownLatch latch = new CountDownLatch(2);

                final TimeValue ackTimeout = TimeValue.timeValueMillis(randomInt(100));

                publisherRef.set((clusterChangedEvent, publishListener, ackListener) -> {
                    publishListener.onResponse(null);
                    ackListener.onCommit(TimeValue.timeValueMillis(ackTimeout.millis() + randomInt(100)));
                    ackListener.onNodeAck(node1, null);
                    ackListener.onNodeAck(node2, null);
                    ackListener.onNodeAck(node3, null);
                });

                clusterManagerService.submitStateUpdateTask("test2", new AckedClusterStateUpdateTask<Void>(null, null) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState).build();
                    }

                    @Override
                    public TimeValue ackTimeout() {
                        return ackTimeout;
                    }

                    @Override
                    public TimeValue timeout() {
                        return null;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        latch.countDown();
                    }

                    @Override
                    protected Void newResponse(boolean acknowledged) {
                        fail();
                        return null;
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }

                    @Override
                    public void onAckTimeout() {
                        latch.countDown();
                    }
                });

                latch.await();
            }
        }
    }

    public void testDeprecatedMasterServiceUpdateTaskThreadName() {
        Thread.currentThread().setName(MasterService.MASTER_UPDATE_THREAD_NAME);
        assertThat(MasterService.assertClusterManagerUpdateThread(), is(Boolean.TRUE));
        assertThrows(AssertionError.class, () -> MasterService.assertNotClusterManagerUpdateThread("test"));
        Thread.currentThread().setName(MasterService.CLUSTER_MANAGER_UPDATE_THREAD_NAME);
        assertThat(MasterService.assertClusterManagerUpdateThread(), is(Boolean.TRUE));
        assertThrows(AssertionError.class, () -> MasterService.assertNotClusterManagerUpdateThread("test"));
        Thread.currentThread().setName("test not cluster manager update thread");
        assertThat(MasterService.assertNotClusterManagerUpdateThread("test"), is(Boolean.TRUE));
        assertThrows(AssertionError.class, () -> MasterService.assertClusterManagerUpdateThread());
    }

    /**
     * Returns the cluster state that the cluster-manager service uses (and that is provided by the discovery layer)
     */
    public static ClusterState discoveryState(ClusterManagerService clusterManagerService) {
        return clusterManagerService.state();
    }

}
