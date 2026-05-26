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

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

public class OpenSearchThreadPoolExecutorTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put("node.name", "es-thread-pool-executor-tests")
            .put("thread_pool.write.size", 1)
            .put("thread_pool.write.queue_size", 0)
            .put("thread_pool.search.size", 1)
            .put("thread_pool.search.queue_size", 1)
            .build();
    }

    public void testRejectedExecutionExceptionContainsNodeName() {
        // we test a fixed and an auto-queue executor but not scaling since it does not reject
        runThreadPoolExecutorTest(1, ThreadPool.Names.WRITE);
        runThreadPoolExecutorTest(2, ThreadPool.Names.SEARCH);

    }

    private void runThreadPoolExecutorTest(final int fill, final String executor) {
        final CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < fill; i++) {
            node().injector().getInstance(ThreadPool.class).executor(executor).execute(() -> {
                try {
                    latch.await();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        final AtomicBoolean rejected = new AtomicBoolean();
        node().injector().getInstance(ThreadPool.class).executor(executor).execute(new AbstractRunnable() {
            @Override
            public void onFailure(final Exception e) {

            }

            @Override
            public void onRejection(final Exception e) {
                rejected.set(true);
                assertThat(e, hasToString(containsString("name = es-thread-pool-executor-tests/" + executor + ", ")));
            }

            @Override
            protected void doRun() throws Exception {

            }
        });

        latch.countDown();
        assertTrue(rejected.get());
    }

    public void testForcePutBypassesQueueCapacity() throws InterruptedException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        OpenSearchThreadPoolExecutor executor = OpenSearchExecutors.newFixed(
            "test-search",
            1,
            5,
            OpenSearchExecutors.daemonThreadFactory("test"),
            threadContext
        );

        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch threadOccupied = new CountDownLatch(1);

        // occupy only one thread
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws InterruptedException {
                threadOccupied.countDown(); // thread 점유 완료 신호
                blockLatch.await();
            }
            @Override
            public void onFailure(Exception e) {}
        });

        // wait until the thread is truly occupied to prevent race condition
        assertTrue(threadOccupied.await(5, TimeUnit.SECONDS));

        // fill queue with max capacity 5
        for (int i = 0; i < 5; i++) {
            executor.execute(new AbstractRunnable() {
                @Override protected void doRun() {}
                @Override public void onFailure(Exception e) {}
            });
        }

        // execute forceTask with forceExecution = true
        AbstractRunnable forceTask = new AbstractRunnable() {
            @Override public boolean isForceExecution() { return true; }
            @Override protected void doRun() {}
            @Override public void onFailure(Exception e) {}
        };

        executor.execute(forceTask);

        // check the bug: capacity(5) exceeded → forcePut ignored back-pressure
        assertThat(
            "forcePut is executed with ignoring queue capacity (back pressure) : Bug",
            executor.getQueue().size(),
            greaterThan(5) // 이 테스트는 forceExecution이 true일때 forceput을 무시하는 테스트. 이거 말고, isForceExecution이 존재할때를 테스트해야함.
        );

        blockLatch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
}
