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

package org.opensearch.cluster.coordination;

import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * Mock single threaded {@link PrioritizedOpenSearchThreadPoolExecutor} based on {@link DeterministicTaskQueue},
 * simulating the behaviour of an executor returned by {@link OpenSearchExecutors#newSinglePrioritizing}.
 */
public class MockSinglePrioritizingExecutor extends PrioritizedOpenSearchThreadPoolExecutor {

    public MockSinglePrioritizingExecutor(String name, DeterministicTaskQueue deterministicTaskQueue, ThreadPool threadPool) {
        super(name, 0, 1, 0L, TimeUnit.MILLISECONDS, r -> {
            // This executor used to override Thread::start method so the actual runnable is
            // being scheduled in the scope of current thread of execution. In JDK-19, the Thread::start
            // is not called anymore (https://bugs.openjdk.org/browse/JDK-8292027) and there is no
            // suitable option to alter the executor's behavior in the similar way. The closest we
            // could get to is to schedule the runnable once the ThreadFactory is being asked to
            // allocate the new thread.
            deterministicTaskQueue.scheduleNow(new Runnable() {
                @Override
                public void run() {
                    try {
                        r.run();
                    } catch (KillWorkerError kwe) {
                        // hacks everywhere
                    }
                }

                @Override
                public String toString() {
                    return r.toString();
                }
            });

            return new Thread(() -> {});
        }, threadPool.getThreadContext(), threadPool.scheduler());
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        // kill worker so that next one will be scheduled, using cached Error instance to not incur the cost of filling in the stack trace
        // on every task
        throw KillWorkerError.INSTANCE;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        // ensures we don't block
        return false;
    }

    private static final class KillWorkerError extends Error {
        private static final KillWorkerError INSTANCE = new KillWorkerError();
    }
}
