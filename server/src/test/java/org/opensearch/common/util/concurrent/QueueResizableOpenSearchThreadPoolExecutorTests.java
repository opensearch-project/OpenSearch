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
    public void testResizeQueueSameSize() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 2000);

        int threads = randomIntBetween(1, 10);
        int measureWindow = randomIntBetween(100, 200);
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizableOpenSearchThreadPoolExecutor executor = new QueueResizableOpenSearchThreadPoolExecutor(
            "test-threadpool",
            threads,
            threads,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            fastWrapper(),
            OpenSearchExecutors.daemonThreadFactory("queuetest"),
            new OpenSearchAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute a task multiple times that takes 1ms
        assertThat(executor.resize(1000), equalTo(1000));
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> { assertThat(queue.capacity(), lessThanOrEqualTo(1000)); });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void testResizeQueueUp() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 2000);

        int threads = randomIntBetween(1, 10);
        int measureWindow = randomIntBetween(100, 200);
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizableOpenSearchThreadPoolExecutor executor = new QueueResizableOpenSearchThreadPoolExecutor(
            "test-threadpool",
            threads,
            threads,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            fastWrapper(),
            OpenSearchExecutors.daemonThreadFactory("queuetest"),
            new OpenSearchAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute a task multiple times that takes 1ms
        assertThat(executor.resize(3000), equalTo(3000));
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> { assertThat(queue.capacity(), greaterThanOrEqualTo(2000)); });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void testResizeQueueDown() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 2000);

        int threads = randomIntBetween(1, 10);
        int measureWindow = randomIntBetween(100, 200);
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizableOpenSearchThreadPoolExecutor executor = new QueueResizableOpenSearchThreadPoolExecutor(
            "test-threadpool",
            threads,
            threads,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            fastWrapper(),
            OpenSearchExecutors.daemonThreadFactory("queuetest"),
            new OpenSearchAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute a task multiple times that takes 1ms
        assertThat(executor.resize(900), equalTo(900));
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> { assertThat(queue.capacity(), lessThanOrEqualTo(900)); });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void testExecutionEWMACalculation() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 100);

        QueueResizableOpenSearchThreadPoolExecutor executor = new QueueResizableOpenSearchThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            fastWrapper(),
            OpenSearchExecutors.daemonThreadFactory("queuetest"),
            new OpenSearchAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(30L)); });
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(51L)); });
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(65L)); });
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(75L)); });
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(83L)); });

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    /** Use a runnable wrapper that simulates a task with unknown failures. */
    public void testExceptionThrowingTask() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 100);

        QueueResizableOpenSearchThreadPoolExecutor executor = new QueueResizableOpenSearchThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            exceptionalWrapper(),
            OpenSearchExecutors.daemonThreadFactory("queuetest"),
            new OpenSearchAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        executeTask(executor, 1);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
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

    public class SettableTimedRunnable extends TimedRunnable {
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
