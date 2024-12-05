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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;

public class ScalingThreadPoolTests extends OpenSearchThreadPoolTestCase {

    @ParametersFactory
    public static Collection<Object[]> scalingThreadPools() {
        return ThreadPool.THREAD_POOL_TYPES.entrySet()
            .stream()
            .filter(t -> t.getValue().equals(ThreadPool.ThreadPoolType.SCALING))
            .map(e -> new String[] { e.getKey() })
            .collect(Collectors.toList());
    }

    private final String threadPoolName;

    public ScalingThreadPoolTests(String threadPoolName) {
        this.threadPoolName = threadPoolName;
    }

    public void testScalingThreadPoolConfiguration() throws InterruptedException {
        final Settings.Builder builder = Settings.builder();

        final int core;
        if (randomBoolean()) {
            core = randomIntBetween(0, 8);
            builder.put("thread_pool." + threadPoolName + ".core", core);
        } else {
            core = "generic".equals(threadPoolName) ? 4 : 1; // the defaults
        }

        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        final int maxBasedOnNumberOfProcessors;
        final int processorsUsed;
        if (randomBoolean()) {
            final int processors = randomIntBetween(1, 64);
            maxBasedOnNumberOfProcessors = expectedSize(threadPoolName, processors);
            builder.put("node.processors", processors);
            processorsUsed = processors;
        } else {
            maxBasedOnNumberOfProcessors = expectedSize(threadPoolName, availableProcessors);
            processorsUsed = availableProcessors;
        }

        final int expectedMax;
        if (maxBasedOnNumberOfProcessors < core || randomBoolean()) {
            expectedMax = randomIntBetween(Math.max(1, core), 16);
            builder.put("thread_pool." + threadPoolName + ".max", expectedMax);
        } else {
            expectedMax = maxBasedOnNumberOfProcessors;
        }

        final long keepAlive;
        if (randomBoolean()) {
            keepAlive = randomIntBetween(1, 300);
            builder.put("thread_pool." + threadPoolName + ".keep_alive", keepAlive + "s");
        } else {
            keepAlive = "generic".equals(threadPoolName) ? 30 : 300; // the defaults
        }

        runScalingThreadPoolTest(builder.build(), (clusterSettings, threadPool) -> {
            final Executor executor = threadPool.executor(threadPoolName);
            assertThat(executor, instanceOf(OpenSearchThreadPoolExecutor.class));
            final OpenSearchThreadPoolExecutor openSearchThreadPoolExecutor = (OpenSearchThreadPoolExecutor) executor;
            final ThreadPool.Info info = info(threadPool, threadPoolName);

            assertThat(info.getName(), equalTo(threadPoolName));
            assertThat(info.getThreadPoolType(), equalTo(ThreadPool.ThreadPoolType.SCALING));

            assertThat(info.getKeepAlive().seconds(), equalTo(keepAlive));
            assertThat(openSearchThreadPoolExecutor.getKeepAliveTime(TimeUnit.SECONDS), equalTo(keepAlive));

            assertNull(info.getQueueSize());
            assertThat(openSearchThreadPoolExecutor.getQueue().remainingCapacity(), equalTo(Integer.MAX_VALUE));

            assertThat(info.getMin(), equalTo(core));
            assertThat(openSearchThreadPoolExecutor.getCorePoolSize(), equalTo(core));
            assertThat(info.getMax(), equalTo(expectedMax));
            assertThat(openSearchThreadPoolExecutor.getMaximumPoolSize(), equalTo(expectedMax));
        });

        if (processorsUsed > availableProcessors) {
            assertWarnings(
                "setting [node.processors] to value ["
                    + processorsUsed
                    + "] which is more than available processors ["
                    + availableProcessors
                    + "] is deprecated"
            );
        }
    }

    private int expectedSize(final String threadPoolName, final int numberOfProcessors) {
        final Map<String, Function<Integer, Integer>> sizes = new HashMap<>();
        sizes.put(ThreadPool.Names.GENERIC, n -> ThreadPool.boundedBy(4 * n, 128, 512));
        sizes.put(ThreadPool.Names.MANAGEMENT, n -> 5);
        sizes.put(ThreadPool.Names.FLUSH, ThreadPool::halfAllocatedProcessorsMaxFive);
        sizes.put(ThreadPool.Names.REFRESH, ThreadPool::halfAllocatedProcessorsMaxTen);
        sizes.put(ThreadPool.Names.WARMER, ThreadPool::halfAllocatedProcessorsMaxFive);
        sizes.put(ThreadPool.Names.SNAPSHOT, ThreadPool::halfAllocatedProcessorsMaxFive);
        sizes.put(ThreadPool.Names.SNAPSHOT_DELETION, n -> ThreadPool.boundedBy(4 * n, 64, 256));
        sizes.put(ThreadPool.Names.FETCH_SHARD_STARTED, ThreadPool::twiceAllocatedProcessors);
        sizes.put(ThreadPool.Names.FETCH_SHARD_STORE, ThreadPool::twiceAllocatedProcessors);
        sizes.put(ThreadPool.Names.TRANSLOG_TRANSFER, ThreadPool::halfAllocatedProcessors);
        sizes.put(ThreadPool.Names.TRANSLOG_SYNC, n -> 4 * n);
        sizes.put(ThreadPool.Names.REMOTE_PURGE, ThreadPool::halfAllocatedProcessors);
        sizes.put(ThreadPool.Names.REMOTE_REFRESH_RETRY, ThreadPool::halfAllocatedProcessors);
        sizes.put(ThreadPool.Names.REMOTE_RECOVERY, ThreadPool::twiceAllocatedProcessors);
        sizes.put(ThreadPool.Names.REMOTE_STATE_READ, n -> ThreadPool.boundedBy(4 * n, 4, 32));
        return sizes.get(threadPoolName).apply(numberOfProcessors);
    }

    public void testScalingThreadPoolIsBounded() throws InterruptedException {
        final int size = randomIntBetween(32, 512);
        final Settings settings = Settings.builder().put("thread_pool." + threadPoolName + ".max", size).build();
        runScalingThreadPoolTest(settings, (clusterSettings, threadPool) -> {
            final CountDownLatch latch = new CountDownLatch(1);
            final int numberOfTasks = 2 * size;
            final CountDownLatch taskLatch = new CountDownLatch(numberOfTasks);
            for (int i = 0; i < numberOfTasks; i++) {
                threadPool.executor(threadPoolName).execute(() -> {
                    try {
                        latch.await();
                        taskLatch.countDown();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            final ThreadPoolStats.Stats stats = stats(threadPool, threadPoolName);
            assertThat(stats.getQueue(), equalTo(numberOfTasks - size));
            assertThat(stats.getLargest(), equalTo(size));
            latch.countDown();
            try {
                taskLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testScalingThreadPoolThreadsAreTerminatedAfterKeepAlive() throws InterruptedException {
        final int min = "generic".equals(threadPoolName) ? 4 : 1;
        final Settings settings = Settings.builder()
            .put("thread_pool." + threadPoolName + ".max", 128)
            .put("thread_pool." + threadPoolName + ".keep_alive", "1ms")
            .build();
        runScalingThreadPoolTest(settings, ((clusterSettings, threadPool) -> {
            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch taskLatch = new CountDownLatch(128);
            for (int i = 0; i < 128; i++) {
                threadPool.executor(threadPoolName).execute(() -> {
                    try {
                        latch.await();
                        taskLatch.countDown();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            int threads = stats(threadPool, threadPoolName).getThreads();
            assertEquals(128, threads);
            latch.countDown();
            // this while loop is the core of this test; if threads
            // are correctly idled down by the pool, the number of
            // threads in the pool will drop to the min for the pool
            // but if threads are not correctly idled down by the pool,
            // this test will just timeout waiting for them to idle
            // down
            do {
                spinForAtLeastOneMillisecond();
            } while (stats(threadPool, threadPoolName).getThreads() > min);
            try {
                taskLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public void runScalingThreadPoolTest(final Settings settings, final BiConsumer<ClusterSettings, ThreadPool> consumer)
        throws InterruptedException {
        ThreadPool threadPool = null;
        try {
            final String test = Thread.currentThread().getStackTrace()[2].getMethodName();
            final Settings nodeSettings = Settings.builder().put(settings).put("node.name", test).build();
            threadPool = new ThreadPool(nodeSettings);
            final ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            consumer.accept(clusterSettings, threadPool);
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }
}
