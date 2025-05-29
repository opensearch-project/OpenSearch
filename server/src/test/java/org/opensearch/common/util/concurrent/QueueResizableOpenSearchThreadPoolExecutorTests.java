/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests for the automatic queue resizing of the {@code QueueResizableOpenSearchThreadPoolExecutor}
 * based on the time taken for each event.
 */
public class QueueResizableOpenSearchThreadPoolExecutorTests extends OpenSearchTestCase {
    private QueueResizableOpenSearchThreadPoolExecutor executor;
    private ResizableBlockingQueue<Runnable> queue;
    private int measureWindow;

    private void createExecutor(int queueSize, Function<Runnable, WrappedRunnable> runnableWrapper) {
        int threads = randomIntBetween(1, 10);
        measureWindow = randomIntBetween(100, 200);
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        this.queue = new ResizableBlockingQueue<>(ConcurrentCollections.newBlockingQueue(), queueSize);
        this.executor = new QueueResizableOpenSearchThreadPoolExecutor(
            "test-threadpool",
            threads,
            threads,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            runnableWrapper,
            OpenSearchExecutors.daemonThreadFactory("queuetest"),
            new OpenSearchAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);
    }

    @After
    public void stopExecutor() {
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }

    public void testResizeQueueSameSize() throws Exception {
        createExecutor(2000, fastWrapper());

        // Execute a task multiple times that takes 1ms
        assertThat(executor.resize(1000), equalTo(1000));
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> assertThat(queue.capacity(), lessThanOrEqualTo(1000)));
    }

    public void testResizeQueueUp() throws Exception {
        createExecutor(2000, fastWrapper());
        // Execute a task multiple times that takes 1ms
        assertThat(executor.resize(3000), equalTo(3000));
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> assertThat(queue.capacity(), greaterThanOrEqualTo(2000)));
    }

    public void testResizeQueueDown() throws Exception {
        createExecutor(2000, fastWrapper());
        // Execute a task multiple times that takes 1ms
        assertThat(executor.resize(1500), equalTo(1500));
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> assertThat(queue.capacity(), lessThanOrEqualTo(1500)));
    }

    public void testExecutionEWMACalculation() throws Exception {
        createExecutor(100, fastWrapper());
        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        executeTask(executor, 1);
        assertBusy(() -> assertThat((long) executor.getTaskExecutionEWMA(), equalTo(30L)));
        executeTask(executor, 1);
        assertBusy(() -> assertThat((long) executor.getTaskExecutionEWMA(), equalTo(51L)));
        executeTask(executor, 1);
        assertBusy(() -> assertThat((long) executor.getTaskExecutionEWMA(), equalTo(65L)));
        executeTask(executor, 1);
        assertBusy(() -> assertThat((long) executor.getTaskExecutionEWMA(), equalTo(75L)));
        executeTask(executor, 1);
        assertBusy(() -> assertThat((long) executor.getTaskExecutionEWMA(), equalTo(83L)));
    }

    /** Use a runnable wrapper that simulates a task with unknown failures. */
    public void testExceptionThrowingTask() {
        createExecutor(100, exceptionalWrapper());
        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        executeTask(executor, 1);
    }

    private Function<Runnable, WrappedRunnable> fastWrapper() {
        return (runnable) -> new SettableTimedRunnable(TimeUnit.NANOSECONDS.toNanos(100), false);
    }

    /**
     * The returned function outputs a WrappedRunnabled that simulates the case
     * where {@link TimedRunnable#getTotalExecutionNanos()} returns -1 because
     * the job failed or was rejected before it finished.
     */
    private Function<Runnable, WrappedRunnable> exceptionalWrapper() {
        return (runnable) -> new SettableTimedRunnable(TimeUnit.NANOSECONDS.toNanos(-1), true);
    }

    /** Execute a blank task {@code times} times for the executor */
    private void executeTask(QueueResizableOpenSearchThreadPoolExecutor executor, int times) {
        logger.info("--> executing a task [{}] times", times);
        for (int i = 0; i < times; i++) {
            executor.execute(() -> {});
        }
    }

    private static class SettableTimedRunnable extends TimedRunnable {
        private final long timeTaken;
        private final boolean testFailedOrRejected;

        public SettableTimedRunnable(long timeTaken, boolean failedOrRejected) {
            super(() -> {});
            this.timeTaken = timeTaken;
            this.testFailedOrRejected = failedOrRejected;
        }

        @Override
        public long getTotalNanos() {
            return timeTaken;
        }

        @Override
        public long getTotalExecutionNanos() {
            return timeTaken;
        }

        @Override
        public boolean getFailedOrRejected() {
            return testFailedOrRejected;
        }
    }
}
