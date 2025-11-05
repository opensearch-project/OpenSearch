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

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.threadpool.ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING;
import static org.opensearch.threadpool.ThreadPool.assertCurrentMethodIsNotCalledRecursively;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

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
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        AtomicInteger result = new AtomicInteger(0);
        pool.submit(() -> result.set(42)).join();
        assertEquals(42, result.get());
        terminate(threadPool);
    }

    public void testForkJoinPoolRegistration() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ExecutorService pool = threadPool.executor("jvector");
        assertNotNull(pool);
        assertTrue(pool instanceof ForkJoinPool);
        assertEquals(expectedParallelism, ((ForkJoinPool) pool).getParallelism());
        terminate(threadPool);
    }

    public void testForkJoinPoolTaskExecution() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        AtomicInteger result = new AtomicInteger(0);
        pool.submit(() -> result.set(42)).join();
        assertEquals(42, result.get());
        terminate(threadPool);
    }

    public void testForkJoinPoolParallelism() throws Exception {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");

        CountDownLatch latch = new CountDownLatch(expectedParallelism);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < expectedParallelism; i++) {
            pool.submit(() -> {
                counter.incrementAndGet();
                latch.countDown();
            });
        }
        latch.await(5, TimeUnit.SECONDS);
        assertEquals(expectedParallelism, counter.get());
        terminate(threadPool);
    }

    public void testForkJoinPoolShutdown() throws Exception {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        threadPool.shutdown();
        assertTrue(pool.isShutdown());
    }

    public void testSubmitAfterShutdownThrows() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        threadPool.shutdown();
        assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> {}));
    }

    public void testForkJoinPoolParallelismOne() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", 1));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        assertEquals(1, pool.getParallelism());
        terminate(threadPool);
    }

    public void testForkJoinPoolHighParallelism() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = 32;
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        assertEquals(expectedParallelism, pool.getParallelism());
        terminate(threadPool);
    }

    public void testForkJoinPoolNullTask() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        assertThrows(NullPointerException.class, () -> pool.submit((Runnable) null));
        threadPool.shutdown();
    }

    public void testForkJoinPoolTaskThrowsException() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        Future<?> future = pool.submit(() -> { throw new RuntimeException("fail!"); });
        assertThrows(ExecutionException.class, () -> future.get());
        threadPool.shutdown();
    }

    public void testForkJoinPoolRecursiveTask() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));
        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        RecursiveTask<Integer> task = new RecursiveTask<>() {
            @Override
            protected Integer compute() {
                return 123;
            }
        };
        int result = pool.invoke(task);
        assertEquals(123, result);
        threadPool.shutdown();
    }

    public void testValidateSettingSkipsForkJoinPool() {
        // Setup minimal settings with node name
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int expectedParallelism = OpenSearchExecutors.allocatedProcessors(settings);
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", expectedParallelism));

        // ForkJoinPool does not support any config, but we still add dummy settings to trigger validateSetting
        Settings forkJoinSettings = Settings.builder().put("jvector.size", "10").build();

        // Should NOT throw, because validateSetting skips ForkJoinPool types
        threadPool.setThreadPool(forkJoinSettings);

        // Clean up
        terminate(threadPool);
    }

    public void testExecutorHolderAcceptsForkJoinPool() {
        ForkJoinPool pool = new ForkJoinPool(1);
        ThreadPool.Info info = new ThreadPool.Info("jvector", ThreadPool.ThreadPoolType.FORK_JOIN, 1);
        ThreadPool.ExecutorHolder holder = new ThreadPool.ExecutorHolder(pool, info);
        assertTrue(holder.executor() instanceof ForkJoinPool);
        assertEquals(info, holder.info);
        pool.shutdown();
    }

    public void testThreadPoolInfoWriteToForkJoinCurrentVersion() throws IOException {
        ThreadPool.Info info = new ThreadPool.Info("jvector", ThreadPool.ThreadPoolType.FORK_JOIN, 1);

        StreamOutput out = new StreamOutput() {
            private Version version = Version.CURRENT;

            @Override
            public void writeByte(byte b) {}

            @Override
            public void writeBytes(byte[] b, int offset, int length) {}

            @Override
            public void writeBytes(byte[] b) {}

            @Override
            public void setVersion(Version v) {
                this.version = v;
            }

            @Override
            public Version getVersion() {
                return version;
            }

            @Override
            public void flush() throws IOException {} // required by abstract base class

            @Override
            public void reset() throws IOException {} // required by abstract base class

            @Override
            public void close() throws IOException {} // required by abstract base class
        };
        out.setVersion(Version.CURRENT);

        // This will exercise the normal serialization logic for ForkJoinPool and current version
        info.writeTo(out);
    }

    public void testStatsParallelismConstructorAndToXContent() throws IOException {
        // 1. Test the full constructor and toXContent with parallelism set
        ThreadPoolStats.Stats stats = new ThreadPoolStats.Stats.Builder().name("test")
            .threads(1)
            .queue(2)
            .active(3)
            .rejected(4L)
            .largest(5)
            .completed(6L)
            .waitTimeNanos(7L)
            .parallelism(8)
            .build();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertThat(json, containsString("\"parallelism\":8"));

        // 2. Test with parallelism = -1 (should not output the field)
        stats = new ThreadPoolStats.Stats.Builder().name("test")
            .threads(1)
            .queue(2)
            .active(3)
            .rejected(4L)
            .largest(5)
            .completed(6L)
            .waitTimeNanos(7L)
            .parallelism(-1)
            .build();
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        json = builder.toString();
        assertThat(json.contains("parallelism"), is(false));
    }

    public void testStatsSerializationParallelismVersion() throws IOException {
        // 3. Test serialization for version >= 3.4.0 (parallelism is written and read)
        ThreadPoolStats.Stats statsOut = new ThreadPoolStats.Stats.Builder().name("test")
            .threads(1)
            .queue(2)
            .active(3)
            .rejected(4L)
            .largest(5)
            .completed(6L)
            .waitTimeNanos(7L)
            .parallelism(9)
            .build();
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_4_0);
        statsOut.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_3_4_0);
        ThreadPoolStats.Stats statsIn = new ThreadPoolStats.Stats(in);
        assertThat(statsIn.getName(), equalTo("test"));
        assertThat(statsIn.getThreads(), equalTo(1));
        assertThat(statsIn.getQueue(), equalTo(2));
        assertThat(statsIn.getActive(), equalTo(3));
        assertThat(statsIn.getRejected(), equalTo(4L));
        assertThat(statsIn.getLargest(), equalTo(5));
        assertThat(statsIn.getCompleted(), equalTo(6L));
        assertThat(statsIn.getWaitTimeNanos(), equalTo(7L));
        assertThat(statsIn.getParallelism(), equalTo(9));

        // 4. Test serialization for version < 3.4.0 (parallelism is not written, should be -1)
        out = new BytesStreamOutput();
        out.setVersion(Version.V_3_3_0);
        statsOut.writeTo(out);
        in = out.bytes().streamInput();
        in.setVersion(Version.V_3_3_0);
        statsIn = new ThreadPoolStats.Stats(in);
        assertThat(statsIn.getParallelism(), equalTo(-1));
    }

    public void testValidateSettingThrowsOnUnknownThreadPoolName() {
        ThreadPool threadPool = new TestThreadPool("test");
        try {
            Settings tpSettings = Settings.builder().put("notarealthreadpool.size", 1).build();
            Exception e = expectThrows(IllegalArgumentException.class, () -> threadPool.setThreadPool(tpSettings));
            assertThat(e.getMessage(), containsString("illegal thread_pool name"));
        } finally {
            terminate(threadPool);
        }
    }

    public void testInfoWriteToWritesFixedForResizableOnOldVersion() throws IOException {
        ThreadPool.Info info = new ThreadPool.Info("foo", ThreadPool.ThreadPoolType.RESIZABLE, 1);
        BytesStreamOutput out = new BytesStreamOutput();
        // Use an explicit older version < 3.0.0
        out.setVersion(Version.fromString("2.9.0"));
        info.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("2.9.0"));
        in.readString(); // name
        String typeStr = in.readString();
        assertEquals("fixed", typeStr);
    }

    public void testStatsAndValidateSettingForForkJoinPool() {
        // Register a ForkJoinPool-based executor in ThreadPool
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        int parallelism = 3;
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", parallelism));
        try {
            // --- Cover stats() branch for FORK_JOIN ---
            ThreadPoolStats stats = threadPool.stats();
            boolean found = false;
            for (ThreadPoolStats.Stats stat : stats) {
                if ("jvector".equals(stat.getName())) {
                    found = true;

                    assertEquals(0, stat.getThreads());
                    assertEquals(0, stat.getQueue());
                    assertEquals(0, stat.getActive());
                    assertEquals(0, stat.getRejected());
                    assertEquals(0, stat.getLargest());
                    assertEquals(0, stat.getCompleted());
                    assertEquals(-1, stat.getWaitTimeNanos());
                    assertEquals(parallelism, stat.getParallelism());
                }
            }
            assertTrue("ForkJoinPool stats entry should exist", found);

            // --- Cover validateSetting skip/continue for FORK_JOIN ---
            // We intentionally supply a bogus config for jvector. Should hit the continue branch and NOT throw.
            Settings bogus = Settings.builder().put("jvector.size", "99").build();
            threadPool.setThreadPool(bogus); // Should not throw!

            // Also cover the branch in validateSetting that throws for unknown thread pool name
            Settings unknown = Settings.builder().put("notarealthreadpool.size", 1).build();
            Exception e = expectThrows(IllegalArgumentException.class, () -> threadPool.setThreadPool(unknown));
            assertTrue(e.getMessage().contains("illegal thread_pool name"));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testInfoStreamInputThrowsOnUnknownTypeAndNewVersion() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.CURRENT);
        out.writeString("foo");
        out.writeString("unknown_type");
        out.writeInt(1);
        out.writeInt(1);
        out.writeOptionalTimeValue(null);
        out.writeOptionalWriteable(null);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.CURRENT);
        Exception e = expectThrows(IllegalArgumentException.class, () -> new ThreadPool.Info(in));
        assertTrue(e.getMessage().contains("Unknown ThreadPoolType"));
    }

    public void testStatsSerializationParallelismNegativeValue() throws IOException {
        ThreadPoolStats.Stats statsOut = new ThreadPoolStats.Stats.Builder().name("test")
            .threads(1)
            .queue(2)
            .active(3)
            .rejected(4L)
            .largest(5)
            .completed(6L)
            .waitTimeNanos(7L)
            .parallelism(-1)
            .build();
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_4_0);
        statsOut.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_3_4_0);
        ThreadPoolStats.Stats statsIn = new ThreadPoolStats.Stats(in);
        assertEquals(-1, statsIn.getParallelism());
    }
}
