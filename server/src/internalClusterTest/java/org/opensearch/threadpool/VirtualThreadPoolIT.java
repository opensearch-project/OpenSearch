/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Single-node IT that defines an inline plugin to register a virtual thread-per-task executor ("vt_pool")
 * and verifies it is available on the node and runs tasks on virtual threads.
 */
public class VirtualThreadPoolIT extends OpenSearchSingleNodeTestCase {

    private static final String POOL_NAME = "vt_pool";

    /**
     * Inline test plugin that registers a virtual thread-per-task executor named "vt_pool".
     */
    public static class TestPlugin extends Plugin {
        @Override
        public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
            return List.of(new VirtualExecutorBuilder(POOL_NAME));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // Load the inline plugin into the single-node cluster for this test
        return List.of(TestPlugin.class);
    }

    public void testVirtualThreadPoolExists() throws Exception {
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        ExecutorService executor = threadPool.executor(POOL_NAME);
        assertNotNull(POOL_NAME + " executor should be registered by the test plugin", executor);

        // Tasks must run on virtual threads named with the standard opensearch[node][pool]#N convention
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean isVirtual = new AtomicBoolean();
        final AtomicReference<String> threadName = new AtomicReference<>();
        executor.execute(() -> {
            isVirtual.set(Thread.currentThread().isVirtual());
            threadName.set(Thread.currentThread().getName());
            latch.countDown();
        });
        assertTrue("task should have run", latch.await(10, TimeUnit.SECONDS));
        assertTrue("task should run on a virtual thread", isVirtual.get());

        final String expectedPrefix = OpenSearchExecutors.threadName(node().settings(), POOL_NAME) + "#";
        assertTrue(
            "thread name [" + threadName.get() + "] should start with [" + expectedPrefix + "]",
            threadName.get().startsWith(expectedPrefix)
        );

        // ThreadPool.Info should report VIRTUAL with no size, queue, or keep alive
        ThreadPool.Info info = threadPool.info(POOL_NAME);
        assertNotNull("ThreadPool.Info for " + POOL_NAME + " should exist", info);
        assertEquals(POOL_NAME, info.getName());
        assertEquals("type must be VIRTUAL", ThreadPool.ThreadPoolType.VIRTUAL, info.getThreadPoolType());
        assertEquals("an unbounded pool reports no min", -1, info.getMin());
        assertEquals("an unbounded pool reports no max", -1, info.getMax());
        assertNull("an unbounded pool has no keep alive", info.getKeepAlive());
        assertNull("an unbounded pool has no queue", info.getQueueSize());
    }

    public void testVirtualThreadPoolPreservesThreadContext() throws Exception {
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        final ThreadContext threadContext = threadPool.getThreadContext();
        // stash the context so the header set below does not leak into other tests
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("test-header", "test-value");
            assertEquals(
                "test-value",
                threadPool.executor(POOL_NAME).submit(() -> threadContext.getHeader("test-header")).get(10, TimeUnit.SECONDS)
            );
        }
    }

    public void testVirtualThreadPoolAppearsInNodeStatsAndInfo() {
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);

        ThreadPool.Info info = null;
        for (ThreadPool.Info candidate : threadPool.info()) {
            if (POOL_NAME.equals(candidate.getName())) {
                info = candidate;
                break;
            }
        }
        assertNotNull("virtual pool should be listed in thread pool info", info);

        ThreadPoolStats.Stats stats = null;
        for (ThreadPoolStats.Stats candidate : threadPool.stats()) {
            if (POOL_NAME.equals(candidate.getName())) {
                stats = candidate;
                break;
            }
        }
        assertNotNull("virtual pool should be listed in thread pool stats", stats);
        // a virtual thread-per-task pool has neither workers nor a queue, so these have no referent
        assertEquals(-1, stats.getThreads());
        assertEquals(-1, stats.getQueue());
        assertEquals(-1, stats.getLargest());
        assertEquals(-1L, stats.getRejected());
        // task flow counters are tracked, so they are never negative
        assertTrue("active must not be negative", stats.getActive() >= 0);
        assertTrue("completed must not be negative", stats.getCompleted() >= 0);
    }

    public void testVirtualThreadPoolReportsTaskFlowStats() throws Exception {
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        final long completedBefore = statsFor(threadPool).getCompleted();

        final int taskCount = randomIntBetween(1, 10);
        final CountDownLatch done = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            threadPool.executor(POOL_NAME).execute(done::countDown);
        }
        assertTrue("all tasks should have run", done.await(10, TimeUnit.SECONDS));

        // the counters are updated in a finally block after the task body, so poll rather than asserting immediately
        assertBusy(() -> {
            ThreadPoolStats.Stats stats = statsFor(threadPool);
            assertEquals("completed should advance by the task count", completedBefore + taskCount, stats.getCompleted());
            assertEquals("no tasks should remain in flight", 0, stats.getActive());
        });
    }

    private static ThreadPoolStats.Stats statsFor(ThreadPool threadPool) {
        for (ThreadPoolStats.Stats candidate : threadPool.stats()) {
            if (POOL_NAME.equals(candidate.getName())) {
                return candidate;
            }
        }
        throw new AssertionError("no stats for thread pool [" + POOL_NAME + "]");
    }
}
