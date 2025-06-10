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

package org.opensearch.common.util.concurrent;

import org.opensearch.common.Randomness;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AbstractAsyncTaskTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(AbstractAsyncTaskTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testAutoRepeat() throws Exception {

        boolean shouldRunThrowException = randomBoolean();
        final CyclicBarrier barrier1 = new CyclicBarrier(2); // 1 for runInternal plus 1 for the test sequence
        final CyclicBarrier barrier2 = new CyclicBarrier(2); // 1 for runInternal plus 1 for the test sequence
        final AtomicInteger count = new AtomicInteger();
        AbstractAsyncTask task = new AbstractAsyncTask(logger, threadPool, TimeValue.timeValueMillis(1), true) {

            @Override
            protected boolean mustReschedule() {
                return true;
            }

            @Override
            protected void runInternal() {
                assertTrue("generic threadpool is configured", Thread.currentThread().getName().contains("[generic]"));
                try {
                    barrier1.await();
                } catch (Exception e) {
                    fail("interrupted");
                }
                count.incrementAndGet();
                try {
                    barrier2.await();
                } catch (Exception e) {
                    fail("interrupted");
                }
                if (shouldRunThrowException) {
                    throw new RuntimeException("foo");
                }
            }

            @Override
            protected String getThreadPool() {
                return ThreadPool.Names.GENERIC;
            }
        };

        assertFalse(task.isScheduled());
        task.rescheduleIfNecessary();
        assertTrue(task.isScheduled());
        barrier1.await();
        assertTrue(task.isScheduled());
        barrier2.await();
        assertEquals(1, count.get());
        barrier1.reset();
        barrier2.reset();
        barrier1.await();
        assertTrue(task.isScheduled());
        task.close();
        barrier2.await();
        assertEquals(2, count.get());
        assertTrue(task.isClosed());
        assertFalse(task.isScheduled());
        assertEquals(2, count.get());
    }

    public void testManualRepeat() throws Exception {

        boolean shouldRunThrowException = randomBoolean();
        final CyclicBarrier barrier = new CyclicBarrier(2); // 1 for runInternal plus 1 for the test sequence
        final AtomicInteger count = new AtomicInteger();
        AbstractAsyncTask task = new AbstractAsyncTask(logger, threadPool, TimeValue.timeValueMillis(1), false) {

            @Override
            protected boolean mustReschedule() {
                return true;
            }

            @Override
            protected void runInternal() {
                assertTrue("generic threadpool is configured", Thread.currentThread().getName().contains("[generic]"));
                count.incrementAndGet();
                try {
                    barrier.await();
                } catch (Exception e) {
                    fail("interrupted");
                }
                if (shouldRunThrowException) {
                    throw new RuntimeException("foo");
                }
            }

            @Override
            protected String getThreadPool() {
                return ThreadPool.Names.GENERIC;
            }
        };

        assertFalse(task.isScheduled());
        task.rescheduleIfNecessary();
        barrier.await();
        assertEquals(1, count.get());
        assertFalse(task.isScheduled());
        barrier.reset();
        expectThrows(TimeoutException.class, () -> barrier.await(10, TimeUnit.MILLISECONDS));
        assertEquals(1, count.get());
        barrier.reset();
        task.rescheduleIfNecessary();
        barrier.await();
        assertEquals(2, count.get());
        assertFalse(task.isScheduled());
        assertFalse(task.isClosed());
        task.close();
        assertTrue(task.isClosed());
    }

    public void testCloseWithNoRun() {

        AbstractAsyncTask task = new AbstractAsyncTask(logger, threadPool, TimeValue.timeValueMinutes(10), true) {

            @Override
            protected boolean mustReschedule() {
                return true;
            }

            @Override
            protected void runInternal() {}
        };

        assertFalse(task.isScheduled());
        task.rescheduleIfNecessary();
        assertTrue(task.isScheduled());
        task.close();
        assertTrue(task.isClosed());
        assertFalse(task.isScheduled());
    }

    public void testChangeInterval() throws Exception {

        final CountDownLatch latch = new CountDownLatch(2);

        AbstractAsyncTask task = new AbstractAsyncTask(logger, threadPool, TimeValue.timeValueHours(1), true) {

            @Override
            protected boolean mustReschedule() {
                return latch.getCount() > 0;
            }

            @Override
            protected void runInternal() {
                latch.countDown();
            }
        };

        assertFalse(task.isScheduled());
        task.rescheduleIfNecessary();
        assertTrue(task.isScheduled());
        task.setInterval(TimeValue.timeValueMillis(1));
        assertTrue(task.isScheduled());
        // This should only take 2 milliseconds in ideal conditions, but allow 10 seconds in case of VM stalls
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertBusy(() -> assertFalse(task.isScheduled()));
        task.close();
        assertFalse(task.isScheduled());
        assertTrue(task.isClosed());
    }

    public void testIsScheduledRemainFalseAfterClose() throws Exception {
        int numTasks = between(10, 50);
        List<AbstractAsyncTask> tasks = new ArrayList<>(numTasks);
        AtomicLong counter = new AtomicLong();
        for (int i = 0; i < numTasks; i++) {
            AbstractAsyncTask task = new AbstractAsyncTask(logger, threadPool, TimeValue.timeValueMillis(randomIntBetween(1, 2)), true) {
                @Override
                protected boolean mustReschedule() {
                    return counter.get() <= 1000;
                }

                @Override
                protected void runInternal() {
                    counter.incrementAndGet();
                }
            };
            task.rescheduleIfNecessary();
            tasks.add(task);
        }
        Randomness.shuffle(tasks);
        IOUtils.close(tasks);
        Randomness.shuffle(tasks);
        for (AbstractAsyncTask task : tasks) {
            assertTrue(task.isClosed());
            assertFalse(task.isScheduled());
        }
    }

    public void testGetSleepDurationForFirstRefresh() {
        AbstractAsyncTask task = new AbstractAsyncTask(
            logger,
            threadPool,
            TimeValue.timeValueMillis(randomIntBetween(1, 2)),
            true,
            OpenSearchTestCase::randomBoolean
        ) {
            @Override
            protected boolean mustReschedule() {
                return true;
            }

            @Override
            protected void runInternal() {
                // no-op
            }
        };
        // No exceptions should be thrown
        task.getSleepDuration();
    }
}
