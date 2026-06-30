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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

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

}
