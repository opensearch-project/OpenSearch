/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;

public class ThreadPoolVirtualTests extends OpenSearchTestCase {

    private static final String POOL_NAME = "test_virtual";

    private ThreadPool buildThreadPool() {
        Settings settings = Settings.builder().put("node.name", "testnode").build();
        return new ThreadPool(settings, new VirtualExecutorBuilder(POOL_NAME));
    }

    public void testRegisterVirtualThreadPool() throws Exception {
        ThreadPool threadPool = buildThreadPool();
        try {
            ExecutorService executor = threadPool.executor(POOL_NAME);
            assertNotNull(executor);

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean isVirtual = new AtomicBoolean();
            final AtomicReference<String> threadName = new AtomicReference<>();
            executor.execute(() -> {
                isVirtual.set(Thread.currentThread().isVirtual());
                threadName.set(Thread.currentThread().getName());
                latch.countDown();
            });
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertTrue("task should run on a virtual thread", isVirtual.get());
            assertEquals("opensearch[testnode][" + POOL_NAME + "]#0", threadName.get());
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }

    public void testVirtualThreadPoolPreservesThreadContext() throws Exception {
        ThreadPool threadPool = buildThreadPool();
        try {
            threadPool.getThreadContext().putHeader("test-header", "test-value");
            assertEquals(
                "test-value",
                threadPool.executor(POOL_NAME)
                    .submit(() -> threadPool.getThreadContext().getHeader("test-header"))
                    .get(10, TimeUnit.SECONDS)
            );
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }

    public void testVirtualThreadPoolInfo() {
        ThreadPool threadPool = buildThreadPool();
        try {
            ThreadPool.Info info = null;
            for (ThreadPool.Info candidate : threadPool.info()) {
                if (POOL_NAME.equals(candidate.getName())) {
                    info = candidate;
                    break;
                }
            }
            assertNotNull("virtual thread pool should be reported in thread pool info", info);
            assertThat(info.getThreadPoolType(), is(ThreadPool.ThreadPoolType.VIRTUAL));
            // an unbounded pool reports no size, queue, or keep alive
            assertEquals(-1, info.getMin());
            assertEquals(-1, info.getMax());
            assertNull(info.getKeepAlive());
            assertNull(info.getQueueSize());
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unchecked")
    public void testVirtualThreadPoolInfoXContent() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info(POOL_NAME, ThreadPool.ThreadPoolType.VIRTUAL);
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        info.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        map = (Map<String, Object>) map.get(POOL_NAME);
        assertEquals("virtual", map.get("type"));
        // an unbounded pool renders no size, queue, or keep alive fields
        assertFalse(map.containsKey("size"));
        assertFalse(map.containsKey("queue_size"));
        assertFalse(map.containsKey("core"));
        assertFalse(map.containsKey("max"));
        assertFalse(map.containsKey("keep_alive"));
    }

    public void testVirtualThreadPoolStats() {
        ThreadPool threadPool = buildThreadPool();
        try {
            ThreadPoolStats.Stats stats = statsFor(threadPool);
            // a virtual thread-per-task pool has neither workers nor a queue, so these have no referent
            assertEquals(-1, stats.getThreads());
            assertEquals(-1, stats.getQueue());
            assertEquals(-1, stats.getLargest());
            assertEquals(-1L, stats.getRejected());
            // task flow counters are tracked, and nothing has run yet
            assertEquals(0, stats.getActive());
            assertEquals(0L, stats.getCompleted());
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }

    public void testVirtualThreadPoolTracksCompletedTasks() throws Exception {
        ThreadPool threadPool = buildThreadPool();
        try {
            final int taskCount = randomIntBetween(1, 20);
            final CountDownLatch done = new CountDownLatch(taskCount);
            for (int i = 0; i < taskCount; i++) {
                threadPool.executor(POOL_NAME).execute(done::countDown);
            }
            assertTrue("all tasks should have run", done.await(10, TimeUnit.SECONDS));

            // the last task decrements active and increments completed in a finally block after countDown, so poll
            assertBusy(() -> {
                ThreadPoolStats.Stats stats = statsFor(threadPool);
                assertEquals("all tasks should be counted as completed", taskCount, stats.getCompleted());
                assertEquals("no tasks should remain in flight", 0, stats.getActive());
            });
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }

    public void testVirtualThreadPoolTracksActiveTasks() throws Exception {
        ThreadPool threadPool = buildThreadPool();
        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch started = new CountDownLatch(2);
        try {
            for (int i = 0; i < 2; i++) {
                threadPool.executor(POOL_NAME).execute(() -> {
                    started.countDown();
                    try {
                        block.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertTrue("both tasks should have started", started.await(10, TimeUnit.SECONDS));

            ThreadPoolStats.Stats stats = statsFor(threadPool);
            assertEquals("both blocked tasks should be active", 2, stats.getActive());
            assertEquals("neither blocked task should be completed", 0L, stats.getCompleted());
        } finally {
            block.countDown();
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }

    public void testVirtualThreadPoolCountsTasksThatThrow() throws Exception {
        ThreadPool threadPool = buildThreadPool();
        try {
            // submit() captures the exception in the Future rather than letting it reach the uncaught exception
            // handler, which keeps the failure out of the test framework's uncaught exception check
            Future<?> future = threadPool.executor(POOL_NAME).submit(() -> { throw new RuntimeException("expected"); });
            ExecutionException e = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
            assertEquals("expected", e.getCause().getMessage());

            // a task that throws still finishes, so it must not leak the active count
            assertBusy(() -> {
                ThreadPoolStats.Stats stats = statsFor(threadPool);
                assertEquals("a failed task should still count as completed", 1L, stats.getCompleted());
                assertEquals("a failed task must not leak the active count", 0, stats.getActive());
            });
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }

    private static ThreadPoolStats.Stats statsFor(ThreadPool threadPool) {
        for (ThreadPoolStats.Stats candidate : threadPool.stats()) {
            if (POOL_NAME.equals(candidate.getName())) {
                return candidate;
            }
        }
        throw new AssertionError("no stats for thread pool [" + POOL_NAME + "]");
    }

    public void testVirtualThreadPoolRegistersNoSettings() {
        assertTrue(new VirtualExecutorBuilder(POOL_NAME).getRegisteredSettings().isEmpty());
    }

    public void testVirtualThreadPoolIsNotResizable() {
        ThreadPool threadPool = buildThreadPool();
        try {
            // a virtual pool has no size to update; setThreadPool should skip it rather than fail on a cast
            threadPool.setThreadPool(Settings.builder().put(POOL_NAME + ".size", 5).build());
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }
}
