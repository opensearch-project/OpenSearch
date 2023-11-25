/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReleasableRetryableRefreshListenerTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(ReleasableRetryableRefreshListenerTests.class);

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = new TestThreadPool(getTestName());
    }

    /**
     * This tests that the performAfterRefresh method is being invoked when the afterRefresh method is invoked. We check that the countDownLatch is decreasing as intended to validate that the performAfterRefresh is being invoked.
     */
    public void testPerformAfterRefresh() throws IOException {

        CountDownLatch countDownLatch = new CountDownLatch(2);
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(mock(ThreadPool.class)) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };

        // First invocation of afterRefresh method
        testRefreshListener.afterRefresh(true);
        assertEquals(1, countDownLatch.getCount());

        // Second invocation of afterRefresh method
        testRefreshListener.afterRefresh(true);
        assertEquals(0, countDownLatch.getCount());
        testRefreshListener.drainRefreshes();
    }

    /**
     * This tests that close is acquiring all permits and even if the afterRefresh method is called, it is no-op.
     */
    public void testCloseAfterRefresh() throws IOException {
        final int initialCount = randomIntBetween(10, 100);
        final CountDownLatch countDownLatch = new CountDownLatch(initialCount);
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(mock(ThreadPool.class)) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };

        int refreshCount = randomIntBetween(1, initialCount);
        for (int i = 0; i < refreshCount; i++) {
            testRefreshListener.afterRefresh(true);
        }
        assertEquals(initialCount - refreshCount, countDownLatch.getCount());

        // Closing the refresh listener so that no further afterRefreshes are executed going forward
        testRefreshListener.drainRefreshes();

        for (int i = 0; i < initialCount - refreshCount; i++) {
            testRefreshListener.afterRefresh(true);
        }
        assertEquals(initialCount - refreshCount, countDownLatch.getCount());
    }

    /**
     * This tests that the retry does not get triggered when there are missing configurations or method overrides that empowers the retry to happen.
     */
    public void testNoRetry() throws IOException {
        int initialCount = randomIntBetween(10, 100);
        final CountDownLatch countDownLatch = new CountDownLatch(initialCount);
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(mock(ThreadPool.class)) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertEquals(initialCount - 1, countDownLatch.getCount());
        testRefreshListener.drainRefreshes();

        testRefreshListener = new ReleasableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertEquals(initialCount - 2, countDownLatch.getCount());
        testRefreshListener.drainRefreshes();

        testRefreshListener = new ReleasableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertEquals(initialCount - 3, countDownLatch.getCount());
        testRefreshListener.drainRefreshes();

        testRefreshListener = new ReleasableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(100);
            }

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertEquals(initialCount - 4, countDownLatch.getCount());
        testRefreshListener.drainRefreshes();
    }

    /**
     * This tests that retry gets scheduled and executed when the configurations and method overrides are done properly.
     */
    public void testRetry() throws Exception {
        int initialCount = randomIntBetween(10, 20);
        final CountDownLatch countDownLatch = new CountDownLatch(initialCount);
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(100);
            }

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            protected boolean isRetryEnabled() {
                return true;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertBusy(() -> assertEquals(0, countDownLatch.getCount()));
        testRefreshListener.drainRefreshes();
    }

    /**
     * This tests that once close method is invoked, then even the retries would become no-op.
     */
    public void testCloseWithRetryPending() throws IOException {
        int initialCount = randomIntBetween(10, 20);
        final CountDownLatch countDownLatch = new CountDownLatch(initialCount);
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(100);
            }

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(randomBoolean());
        testRefreshListener.drainRefreshes();
        assertNotEquals(0, countDownLatch.getCount());
        assertRefreshListenerClosed(testRefreshListener);
    }

    public void testCloseWaitsForAcquiringAllPermits() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                countDownLatch.countDown();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        Thread thread = new Thread(() -> {
            try {
                testRefreshListener.afterRefresh(randomBoolean());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        thread.start();
        assertBusy(() -> assertEquals(0, countDownLatch.getCount()));
        testRefreshListener.drainRefreshes();
        assertRefreshListenerClosed(testRefreshListener);
    }

    public void testScheduleRetryAfterClose() throws Exception {
        // This tests that once the listener has been closed, even the retries would not be scheduled.
        final AtomicLong runCount = new AtomicLong();
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                runCount.incrementAndGet();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return TimeValue.timeValueMillis(100);
            }
        };
        Thread thread1 = new Thread(() -> {
            try {
                testRefreshListener.afterRefresh(true);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        Thread thread2 = new Thread(() -> {
            try {
                Thread.sleep(500);
                testRefreshListener.drainRefreshes();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        assertBusy(() -> assertEquals(1, runCount.get()));
        assertRefreshListenerClosed(testRefreshListener);
    }

    public void testConcurrentScheduleRetry() throws Exception {
        // This tests that there can be only 1 retry that can be scheduled at a time.
        final AtomicLong runCount = new AtomicLong();
        final AtomicInteger retryCount = new AtomicInteger(0);
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                retryCount.incrementAndGet();
                runCount.incrementAndGet();
                return retryCount.get() >= 2;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(5000);
            }

            @Override
            protected boolean isRetryEnabled() {
                return true;
            }
        };
        testRefreshListener.afterRefresh(true);
        testRefreshListener.afterRefresh(true);
        assertBusy(() -> assertEquals(3, runCount.get()));
        testRefreshListener.drainRefreshes();
        assertRefreshListenerClosed(testRefreshListener);
    }

    public void testExceptionDuringThreadPoolSchedule() throws Exception {
        // This tests that if there are exceptions while scheduling the task in the threadpool, the retrySchedule boolean
        // is reset properly to allow future scheduling to happen.
        AtomicInteger runCount = new AtomicInteger();
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.schedule(any(), any(), any())).thenThrow(new RuntimeException());
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(mockThreadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                runCount.incrementAndGet();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(100);
            }

            @Override
            protected boolean isRetryEnabled() {
                return true;
            }
        };
        assertThrows(RuntimeException.class, () -> testRefreshListener.afterRefresh(true));
        assertBusy(() -> assertFalse(testRefreshListener.getRetryScheduledStatus()));
        assertEquals(1, runCount.get());
        testRefreshListener.drainRefreshes();
        assertRefreshListenerClosed(testRefreshListener);
    }

    public void testTimeoutDuringClose() throws Exception {
        // This test checks the expected behaviour when the drainRefreshes times out.
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(mock(ThreadPool.class)) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                try {
                    Thread.sleep(TimeValue.timeValueSeconds(2).millis());
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return true;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            TimeValue getDrainTimeout() {
                return TimeValue.timeValueSeconds(1);
            }
        };
        Thread thread1 = new Thread(() -> {
            try {
                testRefreshListener.afterRefresh(true);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        thread1.start();
        assertBusy(() -> assertEquals(0, testRefreshListener.availablePermits()));
        RuntimeException ex = assertThrows(RuntimeException.class, testRefreshListener::drainRefreshes);
        assertEquals("Failed to acquire all permits", ex.getMessage());
        thread1.join();
    }

    public void testThreadInterruptDuringClose() throws Exception {
        // This test checks the expected behaviour when the thread performing the drainRefresh is interrupted.
        CountDownLatch latch = new CountDownLatch(2);
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(mock(ThreadPool.class)) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                try {
                    Thread.sleep(TimeValue.timeValueSeconds(2).millis());
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return true;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            TimeValue getDrainTimeout() {
                return TimeValue.timeValueSeconds(2);
            }
        };
        Thread thread1 = new Thread(() -> {
            try {
                testRefreshListener.afterRefresh(true);
                latch.countDown();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        Thread thread2 = new Thread(() -> {
            RuntimeException ex = assertThrows(RuntimeException.class, testRefreshListener::drainRefreshes);
            assertEquals("Failed to acquire all permits", ex.getMessage());
            latch.countDown();
        });
        thread1.start();
        assertBusy(() -> assertEquals(0, testRefreshListener.availablePermits()));
        thread2.start();
        thread2.interrupt();
        thread1.join();
        thread2.join();
        assertEquals(0, latch.getCount());
    }

    public void testResumeRefreshesAfterDrainRefreshes() {
        // This test checks the expected behaviour when the refresh listener is drained, but then refreshes are resumed again
        // by closing the releasables acquired by calling the drainRefreshes method.
        ReleasableRetryableRefreshListener testRefreshListener = new ReleasableRetryableRefreshListener(mock(ThreadPool.class)) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                return true;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        assertRefreshListenerOpen(testRefreshListener);
        Releasable releasable = testRefreshListener.drainRefreshes();
        assertRefreshListenerClosed(testRefreshListener);
        releasable.close();
        assertRefreshListenerOpen(testRefreshListener);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    private void assertRefreshListenerClosed(ReleasableRetryableRefreshListener testRefreshListener) {
        assertTrue(testRefreshListener.isClosed());
        assertEquals(0, testRefreshListener.availablePermits());
    }

    private void assertRefreshListenerOpen(ReleasableRetryableRefreshListener testRefreshListener) {
        assertFalse(testRefreshListener.isClosed());
        assertEquals(1, testRefreshListener.availablePermits());
    }
}
