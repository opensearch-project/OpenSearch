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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.tasks;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.node.tasks.TransportTasksActionTests;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.RunnableTaskExecutionListener;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.FakeTcpChannel;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.mockito.Mockito.mock;

public class TaskManagerTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private AtomicReference<RunnableTaskExecutionListener> runnableTaskListener;

    @Before
    public void setupThreadPool() {
        runnableTaskListener = new AtomicReference<>();
        threadPool = new TestThreadPool(TransportTasksActionTests.class.getSimpleName(), runnableTaskListener);
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    /**
     * Makes sure that tasks that attempt to store themselves on completion retry if
     * they don't succeed at first.
     */
    public void testResultsServiceRetryTotalTime() {
        Iterator<TimeValue> times = TaskResultsService.STORE_BACKOFF_POLICY.iterator();
        long total = 0;
        while (times.hasNext()) {
            total += times.next().millis();
        }
        assertEquals(600000L, total);
    }

    public void testTrackingChannelTask() throws Exception {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        Set<Task> cancelledTasks = ConcurrentCollections.newConcurrentSet();
        taskManager.setTaskCancellationService(new TaskCancellationService(mock(TransportService.class)) {
            @Override
            void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
                assertThat(reason, equalTo("channel was closed"));
                assertFalse(waitForCompletion);
                assertTrue("task [" + task + "] was cancelled already", cancelledTasks.add(task));
            }
        });
        Map<TcpChannel, Set<Task>> pendingTasks = new HashMap<>();
        Set<Task> expectedCancelledTasks = new HashSet<>();
        FakeTcpChannel[] channels = new FakeTcpChannel[randomIntBetween(1, 10)];
        List<Releasable> stopTrackingTasks = new ArrayList<>();
        for (int i = 0; i < channels.length; i++) {
            channels[i] = new SingleThreadedTcpChannel();
        }
        int iterations = randomIntBetween(1, 200);
        for (int i = 0; i < iterations; i++) {
            final List<Releasable> subset = randomSubsetOf(stopTrackingTasks);
            stopTrackingTasks.removeAll(subset);
            Releasables.close(subset);
            final FakeTcpChannel channel = randomFrom(channels);
            final Task task = taskManager.register("transport", "test", new CancellableRequest(Integer.toString(i)));
            if (channel.isOpen() && randomBoolean()) {
                channel.close();
                expectedCancelledTasks.addAll(pendingTasks.getOrDefault(channel, Collections.emptySet()));
            }
            final Releasable stopTracking = taskManager.startTrackingCancellableChannelTask(channel, (CancellableTask) task);
            if (channel.isOpen()) {
                pendingTasks.computeIfAbsent(channel, k -> new HashSet<>()).add(task);
                stopTrackingTasks.add(() -> {
                    stopTracking.close();
                    assertTrue(pendingTasks.get(channel).remove(task));
                    expectedCancelledTasks.remove(task);
                });
            } else {
                expectedCancelledTasks.add(task);
            }
        }
        assertBusy(() -> assertThat(expectedCancelledTasks, everyItem(in(cancelledTasks))), 30, TimeUnit.SECONDS);
        for (FakeTcpChannel channel : channels) {
            channel.close();
        }
        assertThat(taskManager.numberOfChannelPendingTaskTrackers(), equalTo(0));
    }

    public void testTrackingTaskAndCloseChannelConcurrently() throws Exception {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        Set<CancellableTask> cancelledTasks = ConcurrentCollections.newConcurrentSet();
        taskManager.setTaskCancellationService(new TaskCancellationService(mock(TransportService.class)) {
            @Override
            void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
                assertTrue("task [" + task + "] was cancelled already", cancelledTasks.add(task));
            }
        });
        Set<Task> expectedCancelledTasks = ConcurrentCollections.newConcurrentSet();
        FakeTcpChannel[] channels = new FakeTcpChannel[randomIntBetween(1, 10)];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = new FakeTcpChannel();
        }
        Thread[] threads = new Thread[randomIntBetween(2, 8)];
        Phaser phaser = new Phaser(threads.length);
        for (int t = 0; t < threads.length; t++) {
            String threadName = "thread-" + t;
            threads[t] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                int iterations = randomIntBetween(50, 500);
                for (int i = 0; i < iterations; i++) {
                    final FakeTcpChannel channel = randomFrom(channels);
                    final Task task = taskManager.register("transport", "test", new CancellableRequest(threadName + ":" + i));
                    expectedCancelledTasks.add(task);
                    taskManager.startTrackingCancellableChannelTask(channel, (CancellableTask) task);
                    if (randomInt(100) < 5) {
                        randomFrom(channels).close();
                    }
                }
            });
            threads[t].start();
        }
        for (FakeTcpChannel channel : channels) {
            channel.close();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertBusy(() -> assertThat(cancelledTasks, equalTo(expectedCancelledTasks)), 1, TimeUnit.MINUTES);
        assertThat(taskManager.numberOfChannelPendingTaskTrackers(), equalTo(0));
    }

    static class CancellableRequest extends TransportRequest {
        private final String requestId;

        CancellableRequest(String requestId) {
            this.requestId = requestId;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "request-" + requestId, parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return false;
                }

                @Override
                public String toString() {
                    return getDescription();
                }
            };
        }
    }

    static class SingleThreadedTcpChannel extends FakeTcpChannel {
        private boolean registeredListener = false;

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            if (isOpen()) {
                assertFalse("listener was registered already", registeredListener);
                registeredListener = true;
            }
            super.addCloseListener(listener);
        }
    }
}
