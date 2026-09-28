/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * Single-node IT that defines an inline plugin to register a ForkJoin executor ("jvector")
 * and verifies it is available on the node.
 */
public class ForkJoinPoolIT extends OpenSearchSingleNodeTestCase {

    /**
     * Inline test plugin that registers a ForkJoin-based executor named "jvector"
     * with a fixed parallelism of 9 for deterministic assertions.
     */
    public static class TestPlugin extends Plugin {
        @Override
        public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
            return List.of(new ForkJoinPoolExecutorBuilder("jvector", 9));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // Load the inline plugin into the single-node cluster for this test
        return List.of(TestPlugin.class);
    }

    public void testForkJoinPoolExists() {
        // Obtain the node's ThreadPool and verify the "jvector" executor
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        ExecutorService executor = threadPool.executor("jvector");
        assertNotNull("jvector executor should be registered by the test plugin", executor);
        assertTrue("jvector should be a ForkJoinPool", executor instanceof ForkJoinPool);
        assertEquals("parallelism should be 9", 9, ((ForkJoinPool) executor).getParallelism());

        // Also validate ThreadPool.Info reports FORK_JOIN with expected parallelism (max)
        ThreadPool.Info info = threadPool.info("jvector");
        assertNotNull("ThreadPool.Info for jvector should exist", info);
        assertEquals("jvector", info.getName());
        assertEquals("type must be FORK_JOIN", ThreadPool.ThreadPoolType.FORK_JOIN, info.getThreadPoolType());
        assertEquals("info.max should equal parallelism", 9, info.getMax());
    }
}
