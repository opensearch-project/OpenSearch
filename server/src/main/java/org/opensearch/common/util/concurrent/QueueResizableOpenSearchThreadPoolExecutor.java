/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.ExponentiallyWeightedMovingAverage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * An extension to thread pool executor, which allows to adjusts the queue size of the
 * {@code ResizableBlockingQueue} and tracks EWMA.
 *
 * @opensearch.internal
 */
public final class QueueResizableOpenSearchThreadPoolExecutor extends OpenSearchThreadPoolExecutor
    implements
        EWMATrackingThreadPoolExecutor {

    // This is a random starting point alpha. TODO: revisit this with actual testing and/or make it configurable
    public static double EWMA_ALPHA = 0.3;

    private final BlockingQueue<Runnable> workQueue;
    private final Function<Runnable, WrappedRunnable> runnableWrapper;
    private final ExponentiallyWeightedMovingAverage executionEWMA;

    QueueResizableOpenSearchThreadPoolExecutor(
        String name,
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        Function<Runnable, WrappedRunnable> runnableWrapper,
        ThreadFactory threadFactory,
        XRejectedExecutionHandler handler,
        ThreadContext contextHolder
    ) {
        super(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler, contextHolder);
        this.workQueue = workQueue;
        this.runnableWrapper = runnableWrapper;
        this.executionEWMA = new ExponentiallyWeightedMovingAverage(EWMA_ALPHA, 0);
    }

    @Override
    protected Runnable wrapRunnable(Runnable command) {
        return super.wrapRunnable(this.runnableWrapper.apply(command));
    }

    @Override
    protected Runnable unwrap(Runnable runnable) {
        final Runnable unwrapped = super.unwrap(runnable);
        if (unwrapped instanceof WrappedRunnable) {
            return ((WrappedRunnable) unwrapped).unwrap();
        } else {
            return unwrapped;
        }
    }

    /**
     * Returns the exponentially weighted moving average of the task execution time
     */
    @Override
    public double getTaskExecutionEWMA() {
        return executionEWMA.getAverage();
    }

    /**
     * Returns the current queue size (operations that are queued)
     */
    @Override
    public int getCurrentQueueSize() {
        return workQueue.size();
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        // A task has been completed, it has left the building. We should now be able to get the
        // total time as a combination of the time in the queue and time spent running the task. We
        // only want runnables that did not throw errors though, because they could be fast-failures
        // that throw off our timings, so only check when t is null.
        assert super.unwrap(r) instanceof TimedRunnable : "expected only TimedRunnables in queue";
        final TimedRunnable timedRunnable = (TimedRunnable) super.unwrap(r);
        final boolean failedOrRejected = timedRunnable.getFailedOrRejected();

        final long taskExecutionNanos = timedRunnable.getTotalExecutionNanos();
        assert taskExecutionNanos >= 0 || (failedOrRejected && taskExecutionNanos == -1)
            : "expected task to always take longer than 0 nanoseconds or have '-1' failure code, got: "
                + taskExecutionNanos
                + ", failedOrRejected: "
                + failedOrRejected;

        if (taskExecutionNanos != -1) {
            // taskExecutionNanos may be -1 if the task threw an exception
            executionEWMA.addValue(taskExecutionNanos);
        }
    }

    /**
     * Resizes the work queue capacity of the pool
     * @param capacity the new capacity
     */
    public synchronized int resize(int capacity) {
        if (workQueue instanceof ResizableBlockingQueue) {
            final ResizableBlockingQueue<Runnable> resizableWorkQueue = (ResizableBlockingQueue<Runnable>) workQueue;
            final int currentCapacity = resizableWorkQueue.capacity();
            // Reusing adjustCapacity method instead of introducing the new one
            if (currentCapacity < capacity) {
                return resizableWorkQueue.adjustCapacity(capacity + 1, StrictMath.abs(capacity - currentCapacity), capacity, capacity);
            } else {
                return resizableWorkQueue.adjustCapacity(capacity - 1, StrictMath.abs(capacity - currentCapacity), capacity, capacity);
            }
        } else {
            return workQueue.size();
        }
    }
}
