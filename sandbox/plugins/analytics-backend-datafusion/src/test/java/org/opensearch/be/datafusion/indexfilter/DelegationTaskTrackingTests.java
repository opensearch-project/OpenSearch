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
 */
public class DelegationTaskTrackingTests extends OpenSearchTestCase {

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
        FilterTreeCallbacks.setHandle(null);
        FilterTreeCallbacks.setThreadTracker(null);
    }

    @Override
    public void tearDown() throws Exception {
        FilterTreeCallbacks.setThreadTracker(null);
        FilterTreeCallbacks.setHandle(null);
        terminate(threadPool);
        super.tearDown();
    }

    /**
     * Tests the full production wiring: setDelegationThreadTracker via SPI, then
     * all three callback methods (createProvider, createCollector, collectDocs)
     * on a foreign thread. Verifies the thread is tracked against the task.
     */
    public void testAllCallbackMethodsTrackedOnForeignThread() throws Exception {
        AnalyticsShardTask task = createAndTrackTask(1);

        var backendPlugin = new org.opensearch.be.datafusion.DataFusionAnalyticsBackendPlugin(null);
        backendPlugin.setDelegationThreadTracker(createTracker(task.getId()));
        FilterTreeCallbacks.setHandle(new MockHandle(new long[] { 0xCAFEL }));

        CountDownLatch done = new CountDownLatch(1);
        Thread foreignThread = new Thread(() -> {
            int pk = FilterTreeCallbacks.createProvider(1);
            int ck = FilterTreeCallbacks.createCollector(pk, 0, 0, 64);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment buf = arena.allocate(Long.BYTES);
                FilterTreeCallbacks.collectDocs(ck, 0, 64, buf, 1);
            }
            FilterTreeCallbacks.releaseCollector(ck);
            FilterTreeCallbacks.releaseProvider(pk);
            done.countDown();
        }, "test-tokio-worker");
        foreignThread.start();
        assertTrue(done.await(5, TimeUnit.SECONDS));

        backendPlugin.setDelegationThreadTracker(null);
        trackingService.stopTracking(task);

        Map<Long, List<ThreadResourceInfo>> stats = task.getResourceStats();
        assertTrue("Foreign thread should be tracked. Got threads: " + stats.keySet(), stats.containsKey(foreignThread.threadId()));
    }

    /**
     * Tests that clearing the thread tracker stops attribution. After clearing,
     * callbacks on a new thread should NOT be attributed to the old task.
     */
    public void testClearTaskTrackingStopsAttribution() throws Exception {
        AnalyticsShardTask task = createAndTrackTask(2);

        FilterTreeCallbacks.setThreadTracker(createTracker(task.getId()));
        FilterTreeCallbacks.setHandle(new MockHandle(new long[] { 1L }));

        // Clear tracking BEFORE running callbacks
        FilterTreeCallbacks.setThreadTracker(null);

        CountDownLatch done = new CountDownLatch(1);
        Thread foreignThread = new Thread(() -> {
            int pk = FilterTreeCallbacks.createProvider(1);
            int ck = FilterTreeCallbacks.createCollector(pk, 0, 0, 64);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment buf = arena.allocate(Long.BYTES);
                FilterTreeCallbacks.collectDocs(ck, 0, 64, buf, 1);
            }
            FilterTreeCallbacks.releaseCollector(ck);
            FilterTreeCallbacks.releaseProvider(pk);
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

        FilterTreeCallbacks.setThreadTracker(createTracker(task.getId()));
        FilterTreeCallbacks.setHandle(new MockHandle(new long[] { 0xFFL }));

        int threadCount = 4;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    int pk = FilterTreeCallbacks.createProvider(1);
                    int ck = FilterTreeCallbacks.createCollector(pk, 0, 0, 64);
                    try (Arena arena = Arena.ofConfined()) {
                        MemorySegment buf = arena.allocate(Long.BYTES);
                        FilterTreeCallbacks.collectDocs(ck, 0, 64, buf, 1);
                    }
                    FilterTreeCallbacks.releaseCollector(ck);
                    FilterTreeCallbacks.releaseProvider(pk);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }, "concurrent-worker-" + i);
            threads[i].start();
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));

        FilterTreeCallbacks.setThreadTracker(null);
        trackingService.stopTracking(task);

        Map<Long, List<ThreadResourceInfo>> stats = task.getResourceStats();
        for (Thread t : threads) {
            assertTrue("Thread " + t.getName() + " (id=" + t.threadId() + ") should be tracked", stats.containsKey(t.threadId()));
        }
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
        public int createCollector(int providerKey, int segmentOrd, int minDoc, int maxDoc) {
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
