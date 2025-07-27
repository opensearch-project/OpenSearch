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

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matcher;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests for OpenSearchExecutors and its components like OpenSearchAbortPolicy.
 */
public class OpenSearchExecutorsTests extends OpenSearchTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    private TimeUnit randomTimeUnit() {
        return TimeUnit.values()[between(0, TimeUnit.values().length - 1)];
    }

    private String getName() {
        return getClass().getName() + "/" + getTestName();
    }

    public void testFixedForcedExecution() throws Exception {
        OpenSearchThreadPoolExecutor executor = OpenSearchExecutors.newFixed(
            getName(),
            1,
            1,
            OpenSearchExecutors.daemonThreadFactory("test"),
            threadContext
        );
        final CountDownLatch wait = new CountDownLatch(1);

        final CountDownLatch exec1Wait = new CountDownLatch(1);
        final AtomicBoolean executed1 = new AtomicBoolean();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    wait.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executed1.set(true);
                exec1Wait.countDown();
            }
        });

        final CountDownLatch exec2Wait = new CountDownLatch(1);
        final AtomicBoolean executed2 = new AtomicBoolean();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                executed2.set(true);
                exec2Wait.countDown();
            }
        });

        final AtomicBoolean executed3 = new AtomicBoolean();
        final CountDownLatch exec3Wait = new CountDownLatch(1);
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                executed3.set(true);
                exec3Wait.countDown();
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        wait.countDown();

        exec1Wait.await();
        exec2Wait.await();
        exec3Wait.await();

        assertThat(executed1.get(), equalTo(true));
        assertThat(executed2.get(), equalTo(true));
        assertThat(executed3.get(), equalTo(true));

        executor.shutdownNow();
    }

    public void testFixedRejected() throws Exception {
        OpenSearchThreadPoolExecutor executor = OpenSearchExecutors.newFixed(
            getName(),
            1,
            1,
            OpenSearchExecutors.daemonThreadFactory("test"),
            threadContext
        );
        final CountDownLatch wait = new CountDownLatch(1);

        final CountDownLatch exec1Wait = new CountDownLatch(1);
        final AtomicBoolean executed1 = new AtomicBoolean();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    wait.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executed1.set(true);
                exec1Wait.countDown();
            }
        });

        final CountDownLatch exec2Wait = new CountDownLatch(1);
        final AtomicBoolean executed2 = new AtomicBoolean();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                executed2.set(true);
                exec2Wait.countDown();
            }
        });

        final AtomicBoolean executed3 = new AtomicBoolean();
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    executed3.set(true);
                }
            });
            fail("should be rejected...");
        } catch (OpenSearchRejectedExecutionException e) {
            // all is well
        }

        wait.countDown();

        exec1Wait.await();
        exec2Wait.await();

        assertThat(executed1.get(), equalTo(true));
        assertThat(executed2.get(), equalTo(true));
        assertThat(executed3.get(), equalTo(false));

        terminate(executor);
    }

    public void testScaleUp() throws Exception {
        final int min = between(1, 3);
        final int max = between(min + 1, 6);
        final CyclicBarrier barrier = new CyclicBarrier(max + 1);

        ThreadPoolExecutor pool = OpenSearchExecutors.newScaling(
            getClass().getName() + "/" + getTestName(),
            min,
            max,
            between(1, 100),
            randomTimeUnit(),
            OpenSearchExecutors.daemonThreadFactory("test"),
            threadContext
        );
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));

        for (int i = 0; i < max; ++i) {
            final CountDownLatch latch = new CountDownLatch(1);
            pool.execute(() -> {
                latch.countDown();
                try {
                    barrier.await();
                    barrier.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });

            // wait until thread executes this task
            // otherwise, a task might be queued
            latch.await();
        }

        barrier.await();
        assertThat("wrong pool size", pool.getPoolSize(), equalTo(max));
        assertThat("wrong active size", pool.getActiveCount(), equalTo(max));
        barrier.await();
        terminate(pool);
    }

    public void testScaleDown() throws Exception {
        final int min = between(1, 3);
        final int max = between(min + 1, 6);
        final CyclicBarrier barrier = new CyclicBarrier(max + 1);

        final ThreadPoolExecutor pool = OpenSearchExecutors.newScaling(
            getClass().getName() + "/" + getTestName(),
            min,
            max,
            between(1, 100),
            TimeUnit.MILLISECONDS,
            OpenSearchExecutors.daemonThreadFactory("test"),
            threadContext
        );
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));

        for (int i = 0; i < max; ++i) {
            final CountDownLatch latch = new CountDownLatch(1);
            pool.execute(() -> {
                latch.countDown();
                try {
                    barrier.await();
                    barrier.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });

            // wait until thread executes this task
            // otherwise, a task might be queued
            latch.await();
        }

        barrier.await();
        assertThat("wrong pool size", pool.getPoolSize(), equalTo(max));
        assertThat("wrong active size", pool.getActiveCount(), equalTo(max));
        barrier.await();
        assertBusy(() -> {
            assertThat("wrong active count", pool.getActiveCount(), equalTo(0));
            assertThat("idle threads didn't shrink below max. (" + pool.getPoolSize() + ")", pool.getPoolSize(), lessThan(max));
        });
        terminate(pool);
    }

    /**
     * The test case is adapted from https://bugs.openjdk.org/browse/JDK-8323659 reproducer.
     */
    public void testScaleUpWithSpawningTask() throws Exception {
        ThreadPoolExecutor pool = OpenSearchExecutors.newScaling(
            getClass().getName() + "/" + getTestName(),
            0,
            1,
            between(1, 100),
            randomTimeUnit(),
            OpenSearchExecutors.daemonThreadFactory("test"),
            threadContext
        );
        assertThat("Min property", pool.getCorePoolSize(), equalTo(0));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(1));

        final CountDownLatch latch = new CountDownLatch(10);
        class TestTask implements Runnable {
            @Override
            public void run() {
                latch.countDown();
                if (latch.getCount() > 0) {
                    pool.execute(TestTask.this);
                }
            }
        }
        pool.execute(new TestTask());
        latch.await();

        assertThat("wrong pool size", pool.getPoolSize(), lessThanOrEqualTo(1));
        assertThat("wrong active size", pool.getActiveCount(), lessThanOrEqualTo(1));

        terminate(pool);
    }

    public void testRejectionMessageAndShuttingDownFlag() throws InterruptedException {
        int pool = between(1, 10);
        int queue = between(0, 100);
        int actions = queue + pool;
        final CountDownLatch latch = new CountDownLatch(1);
        OpenSearchThreadPoolExecutor executor = OpenSearchExecutors.newFixed(
            getName(),
            pool,
            queue,
            OpenSearchExecutors.daemonThreadFactory("dummy"),
            threadContext
        );
        try {
            for (int i = 0; i < actions; i++) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            try {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        // Doesn't matter is going to be rejected
                    }

                    @Override
                    public String toString() {
                        return "dummy runnable";
                    }
                });
                fail("Didn't get a rejection when we expected one.");
            } catch (OpenSearchRejectedExecutionException e) {
                assertFalse("Thread pool registering as terminated when it isn't", e.isExecutorShutdown());
                String message = e.getMessage();
                assertThat(message, containsString("of dummy runnable"));
                assertThat(message, containsString("on OpenSearchThreadPoolExecutor[name = " + getName()));
                assertThat(message, containsString("queue capacity = " + queue));
                assertThat(message, containsString("[Running"));
                /*
                 * While you'd expect all threads in the pool to be active when the queue gets long enough to cause rejections this isn't
                 * always the case. Sometimes you'll see "active threads = <pool - 1>", presumably because one of those threads has finished
                 * its current task but has yet to pick up another task. You too can reproduce this by adding the @Repeat annotation to this
                 * test with something like 10000 iterations. I suspect you could see "active threads = <any natural number <= to pool>". So
                 * that is what we assert.
                 */
                @SuppressWarnings("unchecked")
                Matcher<String>[] activeThreads = new Matcher[pool + 1];
                for (int p = 0; p <= pool; p++) {
                    activeThreads[p] = containsString("active threads = " + p);
                }
                assertThat(message, anyOf(activeThreads));
                assertThat(message, containsString("queued tasks = " + queue));
                assertThat(message, containsString("completed tasks = 0"));
            }
        } finally {
            latch.countDown();
            terminate(executor);
        }
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    // Doesn't matter is going to be rejected
                }

                @Override
                public String toString() {
                    return "dummy runnable";
                }
            });
            fail("Didn't get a rejection when we expected one.");
        } catch (OpenSearchRejectedExecutionException e) {
            assertTrue("Thread pool not registering as terminated when it is", e.isExecutorShutdown());
            String message = e.getMessage();
            assertThat(message, containsString("of dummy runnable"));
            assertThat(message, containsString("on OpenSearchThreadPoolExecutor[name = " + getName()));
            assertThat(message, containsString("queue capacity = " + queue));
            assertThat(message, containsString("[Terminated"));
            assertThat(message, containsString("active threads = 0"));
            assertThat(message, containsString("queued tasks = 0"));
            assertThat(message, containsString("completed tasks = " + actions));
        }
    }

    public void testInheritContext() throws InterruptedException {
        int pool = between(1, 10);
        int queue = between(0, 100);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch executed = new CountDownLatch(1);

        threadContext.putHeader("foo", "bar");
        final Integer one = Integer.valueOf(1);
        threadContext.putTransient("foo", one);
        OpenSearchThreadPoolExecutor executor = OpenSearchExecutors.newFixed(
            getName(),
            pool,
            queue,
            OpenSearchExecutors.daemonThreadFactory("dummy"),
            threadContext
        );
        try {
            executor.execute(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    fail();
                }
                assertEquals(threadContext.getHeader("foo"), "bar");
                assertSame(threadContext.getTransient("foo"), one);
                assertNull(threadContext.getHeader("bar"));
                assertNull(threadContext.getTransient("bar"));
                executed.countDown();
            });
            threadContext.putTransient("bar", "boom");
            threadContext.putHeader("bar", "boom");
            latch.countDown();
            executed.await();

        } finally {
            latch.countDown();
            terminate(executor);
        }
    }

    public void testGetTasks() throws InterruptedException {
        int pool = between(1, 10);
        int queue = between(0, 100);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch executed = new CountDownLatch(1);
        OpenSearchThreadPoolExecutor executor = OpenSearchExecutors.newFixed(
            getName(),
            pool,
            queue,
            OpenSearchExecutors.daemonThreadFactory("dummy"),
            threadContext
        );
        try {
            Runnable r = () -> {
                latch.countDown();
                try {
                    executed.await();
                } catch (InterruptedException e) {
                    fail();
                }
            };
            executor.execute(r);
            latch.await();
            executor.getTasks().forEach((runnable) -> assertSame(runnable, r));
            executed.countDown();

        } finally {
            latch.countDown();
            terminate(executor);
        }
    }

    public void testNodeProcessorsBound() {
        runProcessorsBoundTest(OpenSearchExecutorsUtils.NODE_PROCESSORS_SETTING);
    }

    public void testProcessorsBound() {
        runProcessorsBoundTest(OpenSearchExecutorsUtils.PROCESSORS_SETTING);
    }

    private void runProcessorsBoundTest(final Setting<Integer> processorsSetting) {
        final int available = Runtime.getRuntime().availableProcessors();
        final int processors = randomIntBetween(available + 1, Integer.MAX_VALUE);
        final Settings settings = Settings.builder().put(processorsSetting.getKey(), processors).build();
        processorsSetting.get(settings);
        final Setting<?>[] deprecatedSettings;
        if (processorsSetting.getProperties().contains(Setting.Property.Deprecated)) {
            deprecatedSettings = new Setting<?>[] { processorsSetting };
        } else {
            deprecatedSettings = new Setting<?>[0];
        }
        final String expectedWarning = String.format(
            Locale.ROOT,
            "setting [%s] to value [%d] which is more than available processors [%d] is deprecated",
            processorsSetting.getKey(),
            processors,
            available
        );
        assertSettingDeprecationsAndWarnings(deprecatedSettings, expectedWarning);
    }

}
