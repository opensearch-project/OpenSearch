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

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.node.Node;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.threadpool.ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING;
import static org.opensearch.threadpool.ThreadPool.assertCurrentMethodIsNotCalledRecursively;
import static org.hamcrest.CoreMatchers.equalTo;

public class ThreadPoolTests extends OpenSearchTestCase {

    public void testBoundedByBelowMin() {
        int min = randomIntBetween(0, 32);
        int max = randomIntBetween(min + 1, 64);
        int value = randomIntBetween(Integer.MIN_VALUE, min - 1);
        assertThat(ThreadPool.boundedBy(value, min, max), equalTo(min));
    }

    public void testBoundedByAboveMax() {
        int min = randomIntBetween(0, 32);
        int max = randomIntBetween(min + 1, 64);
        int value = randomIntBetween(max + 1, Integer.MAX_VALUE);
        assertThat(ThreadPool.boundedBy(value, min, max), equalTo(max));
    }

    public void testBoundedByBetweenMinAndMax() {
        int min = randomIntBetween(0, 32);
        int max = randomIntBetween(min + 1, 64);
        int value = randomIntBetween(min, max);
        assertThat(ThreadPool.boundedBy(value, min, max), equalTo(value));
    }

    public void testAbsoluteTime() throws Exception {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            long currentTime = System.currentTimeMillis();
            long gotTime = threadPool.absoluteTimeInMillis();
            long delta = Math.abs(gotTime - currentTime);
            // the delta can be large, we just care it is the same order of magnitude
            assertTrue("thread pool cached absolute time " + gotTime + " is too far from real current time " + currentTime, delta < 10000);
        } finally {
            terminate(threadPool);
        }
    }

    public void testEstimatedTimeIntervalSettingAcceptsOnlyZeroAndPositiveTime() {
        Settings settings = Settings.builder().put("thread_pool.estimated_time_interval", -1).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> ESTIMATED_TIME_INTERVAL_SETTING.get(settings));
        assertEquals("failed to parse value [-1] for setting [thread_pool.estimated_time_interval], must be >= [0ms]", e.getMessage());
    }

    int factorial(int n) {
        assertCurrentMethodIsNotCalledRecursively();
        if (n <= 1) {
            return 1;
        } else {
            return n * factorial(n - 1);
        }
    }

    int factorialForked(int n, ExecutorService executor) {
        assertCurrentMethodIsNotCalledRecursively();
        if (n <= 1) {
            return 1;
        }
        return n * FutureUtils.get(executor.submit(() -> factorialForked(n - 1, executor)));
    }

    public void testAssertCurrentMethodIsNotCalledRecursively() {
        expectThrows(AssertionError.class, () -> factorial(between(2, 10)));
        assertThat(factorial(1), equalTo(1)); // is not called recursively
        assertThat(
            expectThrows(AssertionError.class, () -> factorial(between(2, 10))).getMessage(),
            equalTo("org.opensearch.threadpool.ThreadPoolTests#factorial is called recursively")
        );
        TestThreadPool threadPool = new TestThreadPool("test");
        assertThat(factorialForked(1, threadPool.generic()), equalTo(1));
        assertThat(factorialForked(10, threadPool.generic()), equalTo(3628800));
        assertThat(
            expectThrows(AssertionError.class, () -> factorialForked(between(2, 10), OpenSearchExecutors.newDirectExecutorService()))
                .getMessage(),
            equalTo("org.opensearch.threadpool.ThreadPoolTests#factorialForked is called recursively")
        );
        terminate(threadPool);
    }

    public void testInheritContextOnSchedule() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch executed = new CountDownLatch(1);

        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            threadPool.getThreadContext().putHeader("foo", "bar");
            final Integer one = Integer.valueOf(1);
            threadPool.getThreadContext().putTransient("foo", one);
            threadPool.schedule(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    fail();
                }
                assertEquals(threadPool.getThreadContext().getHeader("foo"), "bar");
                assertSame(threadPool.getThreadContext().getTransient("foo"), one);
                assertNull(threadPool.getThreadContext().getHeader("bar"));
                assertNull(threadPool.getThreadContext().getTransient("bar"));
                executed.countDown();
            }, TimeValue.timeValueMillis(randomInt(100)), randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC));
            threadPool.getThreadContext().putTransient("bar", "boom");
            threadPool.getThreadContext().putHeader("bar", "boom");
            latch.countDown();
            executed.await();
        } finally {
            latch.countDown();
            terminate(threadPool);
        }
    }

    public void testThreadPoolResize() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            // increase it
            Settings commonSettings = Settings.builder().put("snapshot.max", "10").put("snapshot.core", "2").put("get.size", "100").build();
            threadPool.setThreadPool(commonSettings);
            ExecutorService executorService = threadPool.executor("snapshot");
            OpenSearchThreadPoolExecutor executor = (OpenSearchThreadPoolExecutor) executorService;
            assertEquals(10, executor.getMaximumPoolSize());
            assertEquals(2, executor.getCorePoolSize());

            executorService = threadPool.executor("get");
            executor = (OpenSearchThreadPoolExecutor) executorService;
            assertEquals(100, executor.getMaximumPoolSize());
            assertEquals(100, executor.getCorePoolSize());

            // decrease it
            commonSettings = Settings.builder().put("snapshot.max", "2").put("snapshot.core", "1").put("get.size", "90").build();
            threadPool.setThreadPool(commonSettings);
            executorService = threadPool.executor("snapshot");
            executor = (OpenSearchThreadPoolExecutor) executorService;
            assertEquals(2, executor.getMaximumPoolSize());
            assertEquals(1, executor.getCorePoolSize());

            executorService = threadPool.executor("get");
            executor = (OpenSearchThreadPoolExecutor) executorService;
            assertEquals(90, executor.getMaximumPoolSize());
            assertEquals(90, executor.getCorePoolSize());
        } finally {
            terminate(threadPool);
        }
    }

    public void testThreadPoolResizeFail() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            Settings commonSettings = Settings.builder().put("snapshot.max", "50").put("snapshot.core", "100").build();
            assertThrows(IllegalArgumentException.class, () -> threadPool.setThreadPool(commonSettings));
        } finally {
            terminate(threadPool);
        }
    }

    public void testOneEighthAllocatedProcessors() {
        assertThat(ThreadPool.oneEighthAllocatedProcessors(1), equalTo(1));
        assertThat(ThreadPool.oneEighthAllocatedProcessors(4), equalTo(1));
        assertThat(ThreadPool.oneEighthAllocatedProcessors(8), equalTo(1));
        assertThat(ThreadPool.oneEighthAllocatedProcessors(32), equalTo(4));
        assertThat(ThreadPool.oneEighthAllocatedProcessors(128), equalTo(16));
    }

    public void testForkJoinPoolRegistrationAndTaskExecution() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int parallelism = 4;
        ThreadPool threadPool = new ThreadPool(
            settings,
            new ForkJoinPoolExecutorBuilder("test_fork_join", parallelism)
        );
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("test_fork_join");
        AtomicInteger result = new AtomicInteger(0);
        pool.submit(() -> result.set(42)).join();
        assertEquals(42, result.get());
        threadPool.shutdown();
        assertTrue(pool.isShutdown());
    }

    public void testForkJoinPoolRegistration() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int parallelism = 4;
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", parallelism));
        ExecutorService pool = threadPool.executor("my_fork_join");
        assertNotNull(pool);
        assertTrue(pool instanceof ForkJoinPool);
        assertEquals(parallelism, ((ForkJoinPool) pool).getParallelism());
        threadPool.shutdown();
    }

    public void testForkJoinPoolTaskExecution() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", 2));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("my_fork_join");
        AtomicInteger result = new AtomicInteger(0);
        pool.submit(() -> result.set(42)).join();
        assertEquals(42, result.get());
        threadPool.shutdown();
    }

    public void testForkJoinPoolParallelism() throws Exception {
        int parallelism = 8;
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", parallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("my_fork_join");

        CountDownLatch latch = new CountDownLatch(parallelism);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < parallelism; i++) {
            pool.submit(() -> {
                counter.incrementAndGet();
                latch.countDown();
            });
        }
        latch.await(5, TimeUnit.SECONDS);
        assertEquals(parallelism, counter.get());
        threadPool.shutdown();
    }

    public void testForkJoinPoolShutdown() throws Exception {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", 2));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("my_fork_join");
        threadPool.shutdown();
        assertTrue(pool.isShutdown());
    }

    public void testSubmitAfterShutdownThrows() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", 2));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("my_fork_join");
        threadPool.shutdown();
        assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> {}));
    }

    public void testForkJoinPoolParallelismOne() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", 1));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("my_fork_join");
        assertEquals(1, pool.getParallelism());
        threadPool.shutdown();
    }

    public void testForkJoinPoolHighParallelism() {
        int parallelism = 64;
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", parallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("my_fork_join");
        assertEquals(parallelism, pool.getParallelism());
        threadPool.shutdown();
    }

    public void testForkJoinPoolNullTask() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", 1));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("my_fork_join");
        assertThrows(NullPointerException.class, () -> pool.submit((Runnable) null));
        threadPool.shutdown();
    }

    public void testForkJoinPoolTaskThrowsException() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", 1));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("my_fork_join");
        Future<?> future = pool.submit(() -> { throw new RuntimeException("fail!"); });
        assertThrows(ExecutionException.class, () -> future.get());
        threadPool.shutdown();
    }

    public void testForkJoinPoolRecursiveTask() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("my_fork_join", 2));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("my_fork_join");
        RecursiveTask<Integer> task = new RecursiveTask<>() {
            @Override
            protected Integer compute() { return 123; }
        };
        int result = pool.invoke(task);
        assertEquals(123, result);
        threadPool.shutdown();
    }

//    public void testForkJoinPoolTypeNotSupportedYet() {
//        Settings settings = Settings.builder()
//            .put("node.name", "test-node")
//            .build();
//
//        Throwable exception = null;
//        ThreadPool threadPool = null;
//        try {
//            threadPool = new ThreadPool(settings);
//            threadPool.registerForkJoinPool("myForkJoinPool", 4);
//        } catch (Exception e) {
//            exception = e;
//        } finally {
//            // Always shutdown to avoid thread leak
//            if (threadPool != null) {
//                threadPool.shutdown();
//            }
//        }
//        assertNotNull("ForkJoinPoolType should not be supported yet", exception);
//    }
}
