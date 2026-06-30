/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.ForkJoinPool;

public class ThreadPoolForkJoinTests extends OpenSearchTestCase {

    public void testRegisterForkJoinPool() {
        // Register a ForkJoinPool thread pool named "jvector" with parallelism 2
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        ThreadPool threadPool = new ThreadPool(settings, new ForkJoinPoolExecutorBuilder("jvector", 2));

        ForkJoinPool pool = (ForkJoinPool) threadPool.executor("jvector");
        assertNotNull(pool);
        assertEquals(2, pool.getParallelism());
        threadPool.shutdown();
    }
}
