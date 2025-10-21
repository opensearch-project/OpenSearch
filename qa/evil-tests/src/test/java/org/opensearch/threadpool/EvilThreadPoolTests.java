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
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ForkJoinPool;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class EvilThreadPoolTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(EvilThreadPoolTests.class.getName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testExecutionErrorOnDefaultThreadPoolTypes() throws InterruptedException {
        for (String executor : ThreadPool.THREAD_POOL_TYPES.keySet()) {
            // ForkJoinPool is skipped here because it does not support all ThreadPoolExecutor features or APIs,
            // and is tested separately in testExecutionErrorOnForkJoinPool.
            if (ThreadPool.THREAD_POOL_TYPES.get(executor) == ThreadPool.ThreadPoolType.FORK_JOIN) {
                continue; // skip FORK_JOIN for these tests
            }
            checkExecutionError(getExecuteRunner(threadPool.executor(executor)));
            checkExecutionError(getSubmitRunner(threadPool.executor(executor)));
            checkExecutionError(getScheduleRunner(executor));
        }
    }

    public void testExecutionErrorOnDirectExecutorService() throws InterruptedException {
        final ExecutorService directExecutorService = OpenSearchExecutors.newDirectExecutorService();
        checkExecutionError(getExecuteRunner(directExecutorService));
        checkExecutionError(getSubmitRunner(directExecutorService));
    }

    public void testExecutionErrorOnFixedESThreadPoolExecutor() throws InterruptedException {
        final OpenSearchThreadPoolExecutor fixedExecutor = OpenSearchExecutors.newFixed("test", 1, 1,
            OpenSearchExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionError(getExecuteRunner(fixedExecutor));
            checkExecutionError(getSubmitRunner(fixedExecutor));
        } finally {
            ThreadPool.terminate(fixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnScalingESThreadPoolExecutor() throws InterruptedException {
        final OpenSearchThreadPoolExecutor scalingExecutor = OpenSearchExecutors.newScaling("test", 1, 1,
            10, TimeUnit.SECONDS, OpenSearchExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionError(getExecuteRunner(scalingExecutor));
            checkExecutionError(getSubmitRunner(scalingExecutor));
        } finally {
            ThreadPool.terminate(scalingExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnAutoQueueFixedESThreadPoolExecutor() throws InterruptedException {
        final OpenSearchThreadPoolExecutor autoQueueFixedExecutor = OpenSearchExecutors.newAutoQueueFixed("test", 1, 1,
            1, 1, 1, TimeValue.timeValueSeconds(10), OpenSearchExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionError(getExecuteRunner(autoQueueFixedExecutor));
            checkExecutionError(getSubmitRunner(autoQueueFixedExecutor));
        } finally {
            ThreadPool.terminate(autoQueueFixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnSinglePrioritizingThreadPoolExecutor() throws InterruptedException {
        final PrioritizedOpenSearchThreadPoolExecutor prioritizedExecutor = OpenSearchExecutors.newSinglePrioritizing("test",
            OpenSearchExecutors.daemonThreadFactory("test"), threadPool.getThreadContext(), threadPool.scheduler());
        try {
            checkExecutionError(getExecuteRunner(prioritizedExecutor));
            checkExecutionError(getSubmitRunner(prioritizedExecutor));
            // bias towards timeout
            checkExecutionError(r -> prioritizedExecutor.execute(delayMillis(r, 10), TimeValue.ZERO, r));
            // race whether timeout or success (but typically biased towards success)
            checkExecutionError(r -> prioritizedExecutor.execute(r, TimeValue.ZERO, r));
            // bias towards no timeout.
            checkExecutionError(r -> prioritizedExecutor.execute(r, TimeValue.timeValueMillis(10), r));
        } finally {
            ThreadPool.terminate(prioritizedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnScheduler() throws InterruptedException {
        final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY);
        try {
            checkExecutionError(getExecuteRunner(scheduler));
            checkExecutionError(getSubmitRunner(scheduler));
            checkExecutionError(r -> scheduler.schedule(r, randomFrom(0, 1), TimeUnit.MILLISECONDS));
        } finally {
            Scheduler.terminate(scheduler, 10, TimeUnit.SECONDS);
        }
    }

    private void checkExecutionError(Consumer<Runnable> runner) throws InterruptedException {
        logger.info("checking error for {}", runner);
        final Runnable runnable;
        if (randomBoolean()) {
            runnable = () -> {
                throw new Error("future error");
            };
        } else {
            runnable = new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {

                }

                @Override
                protected void doRun() {
                    throw new Error("future error");
                }
            };
        }
        runExecutionTest(
            runner,
            runnable,
            true,
            o -> {
                assertTrue(o.isPresent());
                assertThat(o.get(), instanceOf(Error.class));
                assertThat(o.get(), hasToString(containsString("future error")));
            });
    }

    public void testExecutionExceptionOnDefaultThreadPoolTypes() throws InterruptedException {
        for (String executor : ThreadPool.THREAD_POOL_TYPES.keySet()) {
            // ForkJoinPool is skipped here because it does not support all ThreadPoolExecutor features or APIs,
            // and is tested separately in testExecutionErrorOnForkJoinPool.
            if (ThreadPool.THREAD_POOL_TYPES.get(executor) == ThreadPool.ThreadPoolType.FORK_JOIN) {
                continue; // skip FORK_JOIN for these tests
            }
            checkExecutionException(getExecuteRunner(threadPool.executor(executor)), true);

            // here, it's ok for the exception not to bubble up. Accessing the future will yield the exception
            checkExecutionException(getSubmitRunner(threadPool.executor(executor)), false);

            checkExecutionException(getScheduleRunner(executor), true);
        }
    }

    public void testExecutionExceptionOnDirectExecutorService() throws InterruptedException {
        final ExecutorService directExecutorService = OpenSearchExecutors.newDirectExecutorService();
        checkExecutionException(getExecuteRunner(directExecutorService), true);
        checkExecutionException(getSubmitRunner(directExecutorService), false);
    }

    public void testExecutionExceptionOnFixedESThreadPoolExecutor() throws InterruptedException {
        final OpenSearchThreadPoolExecutor fixedExecutor = OpenSearchExecutors.newFixed("test", 1, 1,
            OpenSearchExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionException(getExecuteRunner(fixedExecutor), true);
            checkExecutionException(getSubmitRunner(fixedExecutor), false);
        } finally {
            ThreadPool.terminate(fixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnScalingESThreadPoolExecutor() throws InterruptedException {
        final OpenSearchThreadPoolExecutor scalingExecutor = OpenSearchExecutors.newScaling("test", 1, 1,
            10, TimeUnit.SECONDS, OpenSearchExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            checkExecutionException(getExecuteRunner(scalingExecutor), true);
            checkExecutionException(getSubmitRunner(scalingExecutor), false);
        } finally {
            ThreadPool.terminate(scalingExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnAutoQueueFixedESThreadPoolExecutor() throws InterruptedException {
        final OpenSearchThreadPoolExecutor autoQueueFixedExecutor = OpenSearchExecutors.newAutoQueueFixed("test", 1, 1,
            1, 1, 1, TimeValue.timeValueSeconds(10), OpenSearchExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
        try {
            // fixed_auto_queue_size wraps stuff into TimedRunnable, which is an AbstractRunnable
            checkExecutionException(getExecuteRunner(autoQueueFixedExecutor), true);
            checkExecutionException(getSubmitRunner(autoQueueFixedExecutor), false);
        } finally {
            ThreadPool.terminate(autoQueueFixedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnSinglePrioritizingThreadPoolExecutor() throws InterruptedException {
        final PrioritizedOpenSearchThreadPoolExecutor prioritizedExecutor = OpenSearchExecutors.newSinglePrioritizing("test",
            OpenSearchExecutors.daemonThreadFactory("test"), threadPool.getThreadContext(), threadPool.scheduler());
        try {
            checkExecutionException(getExecuteRunner(prioritizedExecutor), true);
            checkExecutionException(getSubmitRunner(prioritizedExecutor), false);

            // bias towards timeout
            checkExecutionException(r -> prioritizedExecutor.execute(delayMillis(r, 10), TimeValue.ZERO, r), true);
            // race whether timeout or success (but typically biased towards success)
            checkExecutionException(r -> prioritizedExecutor.execute(r, TimeValue.ZERO, r), true);
            // bias towards no timeout.
            checkExecutionException(r -> prioritizedExecutor.execute(r, TimeValue.timeValueMillis(10), r), true);
        } finally {
            ThreadPool.terminate(prioritizedExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionExceptionOnScheduler() throws InterruptedException {
        final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY);
        try {
            checkExecutionException(getExecuteRunner(scheduler), true);
            // while submit does return a Future, we choose to log exceptions anyway,
            // since this is the semi-internal SafeScheduledThreadPoolExecutor that is being used,
            // which also logs exceptions for schedule calls.
            checkExecutionException(getSubmitRunner(scheduler), true);
            checkExecutionException(r -> scheduler.schedule(r, randomFrom(0, 1), TimeUnit.MILLISECONDS), true);
        } finally {
            Scheduler.terminate(scheduler, 10, TimeUnit.SECONDS);
        }
    }

    private Runnable delayMillis(Runnable r, int ms) {
        return () -> {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            r.run();
        };
    }

    private void checkExecutionException(Consumer<Runnable> runner, boolean expectException) throws InterruptedException {
        final Runnable runnable;
        final boolean willThrow;
        if (randomBoolean()) {
            logger.info("checking direct exception for {}", runner);
            runnable = () -> {
                throw new IllegalStateException("future exception");
            };
            willThrow = expectException;
        } else {
            logger.info("checking abstract runnable exception for {}", runner);
            runnable = new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {

                }

                @Override
                protected void doRun() {
                    throw new IllegalStateException("future exception");
                }
            };
            willThrow = false;
        }
        runExecutionTest(
            runner,
            runnable,
            willThrow,
            o -> {
                assertEquals(willThrow, o.isPresent());
                if (willThrow) {
                    if (o.get() instanceof Error) throw (Error) o.get();
                    assertThat(o.get(), instanceOf(IllegalStateException.class));
                    assertThat(o.get(), hasToString(containsString("future exception")));
                }
            });
    }

    Consumer<Runnable> getExecuteRunner(ExecutorService executor) {
        return new Consumer<Runnable>() {
            @Override
            public void accept(Runnable runnable) {
                executor.execute(runnable);
            }

            @Override
            public String toString() {
                return "executor(" + executor + ").execute()";
            }
        };
    }

    Consumer<Runnable> getSubmitRunner(ExecutorService executor) {
        return new Consumer<Runnable>() {
            @Override
            public void accept(Runnable runnable) {
                executor.submit(runnable);
            }

            @Override
            public String toString() {
                return "executor(" + executor + ").submit()";
            }
        };
    }

    Consumer<Runnable> getScheduleRunner(String executor) {
        return new Consumer<Runnable>() {
            @Override
            public void accept(Runnable runnable) {
                threadPool.schedule(runnable, randomFrom(TimeValue.ZERO, TimeValue.timeValueMillis(1)), executor);
            }

            @Override
            public String toString() {
                return "schedule(" + executor + ")";
            }
        };
    }

    private void runExecutionTest(
        final Consumer<Runnable> runner,
        final Runnable runnable,
        final boolean expectThrowable,
        final Consumer<Optional<Throwable>> consumer) throws InterruptedException {
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        final CountDownLatch uncaughtExceptionHandlerLatch = new CountDownLatch(1);

        try {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                assertTrue(expectThrowable);
                assertTrue("Only one message allowed", throwableReference.compareAndSet(null, e));
                uncaughtExceptionHandlerLatch.countDown();
            });

            final CountDownLatch supplierLatch = new CountDownLatch(1);

            try {
                runner.accept(() -> {
                    try {
                        runnable.run();
                    } finally {
                        supplierLatch.countDown();
                    }
                });
            } catch (Throwable t) {
                consumer.accept(Optional.of(t));
                return;
            }

            supplierLatch.await();

            if (expectThrowable) {
                uncaughtExceptionHandlerLatch.await();
            }

            consumer.accept(Optional.ofNullable(throwableReference.get()));
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

    public void testExecutionExceptionOnForkJoinPool() throws InterruptedException {
        ForkJoinPool fjp = new ForkJoinPool();
        try {
            checkExecutionException(getExecuteRunner(fjp), true);
            checkExecutionException(getSubmitRunner(fjp), false);
        } finally {
            fjp.shutdownNow();
            fjp.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testExecutionErrorOnForkJoinPool() throws Exception {
        ForkJoinPool fjp = new ForkJoinPool(8);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        try {
            fjp.execute(() -> {
                try {
                    throw new Error("future error");
                } catch (Throwable t) {
                    thrown.set(t);
                } finally {
                    latch.countDown();
                }
            });

            // Wait up to 5 seconds for the task to complete
            assertTrue("Timeout waiting for ForkJoinPool task", latch.await(5, TimeUnit.SECONDS));

            Throwable error = thrown.get();
            assertNotNull("No error captured from ForkJoinPool task", error);
            assertTrue(error instanceof Error);
            assertEquals("future error", error.getMessage());
        } finally {
            fjp.shutdownNow();
            fjp.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

}
