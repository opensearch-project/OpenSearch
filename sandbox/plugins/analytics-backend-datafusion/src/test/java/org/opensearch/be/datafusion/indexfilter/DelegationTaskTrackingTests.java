/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.ThreadResourceInfo;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests verifying that FilterTreeCallbacks correctly attributes delegation
 * callback work to the AnalyticsShardTask via TaskResourceTrackingService.
 *
 * <p>Tests use contextId=0 directly via {@link FilterTreeCallbacks#register} /
 * {@link FilterTreeCallbacks#unregister} to exercise the per-query binding path.
 */
public class DelegationTaskTrackingTests extends OpenSearchTestCase {

    private static final long TEST_CONTEXT_ID = 0L;

    private ThreadPool threadPool;
    private TaskResourceTrackingService trackingService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName(), new AtomicReference<>());
        trackingService = new TaskResourceTrackingService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        trackingService.setTaskResourceTrackingEnabled(true);
        // Clear any leftover bindings from a previous test.
        FilterTreeCallbacks.unregister(TEST_CONTEXT_ID);
    }

    @Override
    public void tearDown() throws Exception {
        FilterTreeCallbacks.unregister(TEST_CONTEXT_ID);
        terminate(threadPool);
        super.tearDown();
    }

    /**
     * Tests the full production wiring: register handle + tracker, then
     * all five callback methods (createProvider, createCollector, collectDocs,
     * releaseCollector, releaseProvider) on a foreign thread.
     * Verifies the thread is tracked against the task.
     */
    public void testAllCallbackMethodsTrackedOnForeignThread() throws Exception {
        AnalyticsShardTask task = createAndTrackTask(1);

        FilterTreeCallbacks.register(TEST_CONTEXT_ID, new MockHandle(new long[] { 0xCAFEL }), createTracker(task.getId()));

        CountDownLatch done = new CountDownLatch(1);
        Thread foreignThread = new Thread(() -> {
            int pk = FilterTreeCallbacks.createProvider(TEST_CONTEXT_ID, 1);
            int ck = FilterTreeCallbacks.createCollector(TEST_CONTEXT_ID, pk, 0, 0, 64);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment buf = arena.allocate(Long.BYTES);
                FilterTreeCallbacks.collectDocs(TEST_CONTEXT_ID, ck, 0, 64, buf, 1);
            }
            FilterTreeCallbacks.releaseCollector(TEST_CONTEXT_ID, ck);
            FilterTreeCallbacks.releaseProvider(TEST_CONTEXT_ID, pk);
            done.countDown();
        }, "test-tokio-worker");
        foreignThread.start();
        assertTrue(done.await(5, TimeUnit.SECONDS));

        FilterTreeCallbacks.unregister(TEST_CONTEXT_ID);
        trackingService.stopTracking(task);

        Map<Long, List<ThreadResourceInfo>> stats = task.getResourceStats();
        assertTrue("Foreign thread should be tracked. Got threads: " + stats.keySet(), stats.containsKey(foreignThread.threadId()));
    }

    /**
     * Tests that clearing the thread tracker (via unregister) stops attribution.
     * After clearing, callbacks on a new thread should NOT be attributed to the old task.
     */
    public void testClearTaskTrackingStopsAttribution() throws Exception {
        AnalyticsShardTask task = createAndTrackTask(2);

        FilterTreeCallbacks.register(TEST_CONTEXT_ID, new MockHandle(new long[] { 1L }), createTracker(task.getId()));

        // Clear tracking BEFORE running callbacks
        FilterTreeCallbacks.unregister(TEST_CONTEXT_ID);

        CountDownLatch done = new CountDownLatch(1);
        Thread foreignThread = new Thread(() -> {
            // With no binding, createProvider returns -1 (safe no-op); remaining calls
            // also find no binding and return -1 or do nothing.
            int pk = FilterTreeCallbacks.createProvider(TEST_CONTEXT_ID, 1);
            int ck = FilterTreeCallbacks.createCollector(TEST_CONTEXT_ID, pk, 0, 0, 64);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment buf = arena.allocate(Long.BYTES);
                FilterTreeCallbacks.collectDocs(TEST_CONTEXT_ID, ck, 0, 64, buf, 1);
            }
            FilterTreeCallbacks.releaseCollector(TEST_CONTEXT_ID, ck);
            FilterTreeCallbacks.releaseProvider(TEST_CONTEXT_ID, pk);
            done.countDown();
        }, "post-clear-thread");
        foreignThread.start();
        assertTrue(done.await(5, TimeUnit.SECONDS));

        trackingService.stopTracking(task);

        Map<Long, List<ThreadResourceInfo>> stats = task.getResourceStats();
        assertFalse("Thread after clearing tracker should NOT be tracked", stats.containsKey(foreignThread.threadId()));
    }

    /**
     * Tests that multiple concurrent threads calling collectDocs are all tracked.
     */
    public void testConcurrentThreadsAllTracked() throws Exception {
        AnalyticsShardTask task = createAndTrackTask(3);

        FilterTreeCallbacks.register(TEST_CONTEXT_ID, new MockHandle(new long[] { 0xFFL }), createTracker(task.getId()));

        int threadCount = 4;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    int pk = FilterTreeCallbacks.createProvider(TEST_CONTEXT_ID, 1);
                    int ck = FilterTreeCallbacks.createCollector(TEST_CONTEXT_ID, pk, 0, 0, 64);
                    try (Arena arena = Arena.ofConfined()) {
                        MemorySegment buf = arena.allocate(Long.BYTES);
                        FilterTreeCallbacks.collectDocs(TEST_CONTEXT_ID, ck, 0, 64, buf, 1);
                    }
                    FilterTreeCallbacks.releaseCollector(TEST_CONTEXT_ID, ck);
                    FilterTreeCallbacks.releaseProvider(TEST_CONTEXT_ID, pk);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }, "concurrent-worker-" + i);
            threads[i].start();
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));

        FilterTreeCallbacks.unregister(TEST_CONTEXT_ID);
        trackingService.stopTracking(task);

        Map<Long, List<ThreadResourceInfo>> stats = task.getResourceStats();
        for (Thread t : threads) {
            assertTrue("Thread " + t.getName() + " (id=" + t.threadId() + ") should be tracked", stats.containsKey(t.threadId()));
        }
    }

    /**
     * Simulates the production concurrency bug: multiple queries register different
     * handles and trackers, then fire upcalls concurrently on shared threads.
     *
     * Without per-query contextId isolation, this test fails because:
     * - HANDLE race: collectDocs routes to the wrong handle -> returns -1
     * - TRACKER race: trackEnd routes to the wrong task -> IllegalStateException -> AssertionError
     */
    public void testConcurrentQueriesIsolated() throws Exception {
        int queryCount = 4;
        int upcallsPerQuery = 10;

        AnalyticsShardTask[] tasks = new AnalyticsShardTask[queryCount];
        long[] contextIds = new long[queryCount];
        MockHandle[] handles = new MockHandle[queryCount];

        for (int q = 0; q < queryCount; q++) {
            tasks[q] = createAndTrackTask(100 + q);
            contextIds[q] = tasks[q].getId();
            handles[q] = new MockHandle(new long[] { 0xABCD_0000L | q });
            FilterTreeCallbacks.register(contextIds[q], handles[q], createTracker(tasks[q].getId()));
        }

        CyclicBarrier barrier = new CyclicBarrier(queryCount);
        CountDownLatch done = new CountDownLatch(queryCount);
        AssertionError[] errors = new AssertionError[queryCount];
        Thread[] threads = new Thread[queryCount];

        for (int q = 0; q < queryCount; q++) {
            final int queryIdx = q;
            final long ctxId = contextIds[q];
            threads[q] = new Thread(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    for (int i = 0; i < upcallsPerQuery; i++) {
                        int pk = FilterTreeCallbacks.createProvider(ctxId, 1);
                        assertTrue("createProvider should succeed for query " + queryIdx, pk >= 0);

                        int ck = FilterTreeCallbacks.createCollector(ctxId, pk, 0, 0, 64);
                        assertTrue("createCollector should succeed for query " + queryIdx, ck >= 0);

                        try (Arena arena = Arena.ofConfined()) {
                            MemorySegment buf = arena.allocate(Long.BYTES);
                            long words = FilterTreeCallbacks.collectDocs(ctxId, ck, 0, 64, buf, 1);
                            assertTrue("collectDocs should succeed for query " + queryIdx + " (got " + words + ")", words >= 0);

                            long value = buf.getAtIndex(ValueLayout.JAVA_LONG, 0);
                            long expected = 0xABCD_0000L | queryIdx;
                            assertEquals("collectDocs should return this query's data, not another query's", expected, value);
                        }

                        FilterTreeCallbacks.releaseCollector(ctxId, ck);
                        FilterTreeCallbacks.releaseProvider(ctxId, pk);
                    }
                } catch (AssertionError e) {
                    errors[queryIdx] = e;
                } catch (Exception e) {
                    errors[queryIdx] = new AssertionError("Unexpected exception in query " + queryIdx, e);
                } finally {
                    done.countDown();
                }
            }, "concurrent-query-" + q);
            threads[q].start();
        }

        assertTrue("All queries should complete within timeout", done.await(15, TimeUnit.SECONDS));

        for (int q = 0; q < queryCount; q++) {
            FilterTreeCallbacks.unregister(contextIds[q]);
            trackingService.stopTracking(tasks[q]);
        }

        // Check no assertion errors from any query thread
        for (int q = 0; q < queryCount; q++) {
            if (errors[q] != null) {
                throw new AssertionError("Query " + q + " failed", errors[q]);
            }
        }

        // Verify each task was tracked on its own thread (not cross-contaminated)
        for (int q = 0; q < queryCount; q++) {
            Map<Long, List<ThreadResourceInfo>> stats = tasks[q].getResourceStats();
            assertTrue(
                "Task " + tasks[q].getId() + " should have tracking entries on thread " + threads[q].threadId(),
                stats.containsKey(threads[q].threadId())
            );
        }
    }

    /**
     * Demonstrates why per-query isolation is necessary. When multiple queries share
     * the same contextId (simulating the old global singleton behavior), concurrent
     * upcalls read the wrong handle and get incorrect results.
     *
     * This test is expected to FAIL if all queries share a single contextId (the old bug).
     * It passes only because each query has its own contextId.
     */
    public void testSharedContextIdCausesDataCorruption() throws Exception {
        int queryCount = 4;
        long SHARED_CONTEXT_ID = 999L;

        MockHandle[] handles = new MockHandle[queryCount];
        for (int q = 0; q < queryCount; q++) {
            handles[q] = new MockHandle(new long[] { 0xABCD_0000L | q });
        }

        CyclicBarrier barrier = new CyclicBarrier(queryCount);
        CountDownLatch done = new CountDownLatch(queryCount);
        int[] corruptionCount = new int[queryCount];

        Thread[] threads = new Thread[queryCount];
        for (int q = 0; q < queryCount; q++) {
            final int queryIdx = q;
            threads[q] = new Thread(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    for (int i = 0; i < 20; i++) {
                        // Each iteration overwrites the shared binding — last writer wins
                        FilterTreeCallbacks.register(SHARED_CONTEXT_ID, handles[queryIdx], null);

                        int pk = FilterTreeCallbacks.createProvider(SHARED_CONTEXT_ID, 1);
                        if (pk < 0) {
                            corruptionCount[queryIdx]++;
                            continue;
                        }
                        int ck = FilterTreeCallbacks.createCollector(SHARED_CONTEXT_ID, pk, 0, 0, 64);
                        if (ck < 0) {
                            corruptionCount[queryIdx]++;
                            FilterTreeCallbacks.releaseProvider(SHARED_CONTEXT_ID, pk);
                            continue;
                        }

                        try (Arena arena = Arena.ofConfined()) {
                            MemorySegment buf = arena.allocate(Long.BYTES);
                            long words = FilterTreeCallbacks.collectDocs(SHARED_CONTEXT_ID, ck, 0, 64, buf, 1);
                            if (words < 0) {
                                corruptionCount[queryIdx]++;
                            } else {
                                long value = buf.getAtIndex(ValueLayout.JAVA_LONG, 0);
                                long expected = 0xABCD_0000L | queryIdx;
                                if (value != expected) {
                                    corruptionCount[queryIdx]++;
                                }
                            }
                        }
                        FilterTreeCallbacks.releaseCollector(SHARED_CONTEXT_ID, ck);
                        FilterTreeCallbacks.releaseProvider(SHARED_CONTEXT_ID, pk);
                    }
                } catch (Exception e) {
                    corruptionCount[queryIdx] += 100;
                } finally {
                    done.countDown();
                }
            }, "shared-ctx-query-" + q);
            threads[q].start();
        }

        assertTrue(done.await(15, TimeUnit.SECONDS));
        FilterTreeCallbacks.unregister(SHARED_CONTEXT_ID);

        int totalCorruption = 0;
        for (int c : corruptionCount)
            totalCorruption += c;

        assertTrue(
            "With a shared contextId, concurrent queries SHOULD see data corruption "
                + "(wrong handle's data returned). Got "
                + totalCorruption
                + " corruptions out of "
                + (queryCount * 20)
                + " attempts. If this is 0, the test is not exercising the race.",
            totalCorruption > 0
        );
    }

    private DelegationThreadTracker createTracker(long taskId) {
        TaskResourceTrackingService service = trackingService;
        return new DelegationThreadTracker() {
            @Override
            public long trackStart() {
                long threadId = Thread.currentThread().threadId();
                service.taskExecutionStartedOnThread(taskId, threadId);
                return threadId;
            }

            @Override
            public void trackEnd(long threadId) {
                service.taskExecutionFinishedOnThread(taskId, threadId);
            }
        };
    }

    private AnalyticsShardTask createAndTrackTask(long id) {
        AnalyticsShardTask task = new AnalyticsShardTask(
            id,
            "transport",
            "indices:data/read/analytics/fragment",
            "test-task-" + id,
            TaskId.EMPTY_TASK_ID,
            new HashMap<>()
        );
        assertTrue(task.supportsResourceTracking());
        trackingService.startTracking(task);
        return task;
    }

    private static final class MockHandle implements FilterDelegationHandle {
        private final long[] words;
        private int nextKey = 1;

        MockHandle(long[] words) {
            this.words = words;
        }

        @Override
        public int createProvider(int annotationId) {
            return nextKey++;
        }

        @Override
        public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
            return nextKey++;
        }

        @Override
        public int collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
            int n = Math.min(words.length, (int) (out.byteSize() / Long.BYTES));
            for (int i = 0; i < n; i++)
                out.setAtIndex(ValueLayout.JAVA_LONG, i, words[i]);
            return n;
        }

        @Override
        public void releaseCollector(int collectorKey) {}

        @Override
        public void releaseProvider(int providerKey) {}

        @Override
        public void close() {}
    }
}
