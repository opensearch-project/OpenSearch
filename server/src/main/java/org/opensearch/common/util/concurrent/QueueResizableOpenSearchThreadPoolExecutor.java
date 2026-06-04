/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.ExponentiallyWeightedMovingAverage;
import org.opensearch.common.metrics.CounterMetric;

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

    private final ResizableBlockingQueue<Runnable> workQueue;
    private final Function<Runnable, WrappedRunnable> runnableWrapper;
    private final ExponentiallyWeightedMovingAverage executionEWMA;
    private final CounterMetric poolWaitTime;

    /**
     * Create new resizable at runtime thread pool executor
     * @param name thread pool name
     * @param corePoolSize core pool size
     * @param maximumPoolSize maximum pool size
     * @param keepAliveTime keep alive time
     * @param unit time unit for keep alive time
     * @param workQueue work queue
     * @param runnableWrapper runnable wrapper
     * @param threadFactory thread factory
     * @param handler rejected execution handler
     * @param contextHolder context holder
     */
    QueueResizableOpenSearchThreadPoolExecutor(
        String name,
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        ResizableBlockingQueue<Runnable> workQueue,
        Function<Runnable, WrappedRunnable> runnableWrapper,
        ThreadFactory threadFactory,
        XRejectedExecutionHandler handler,
        ThreadContext contextHolder
    ) {
        this(
            name,
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            unit,
            workQueue,
            runnableWrapper,
            threadFactory,
            handler,
            contextHolder,
            EWMA_ALPHA
        );
    }

    /**
     * Create new resizable at runtime thread pool executor
     * @param name thread pool name
     * @param corePoolSize core pool size
     * @param maximumPoolSize maximum pool size
     * @param keepAliveTime keep alive time
     * @param unit time unit for keep alive time
     * @param workQueue work queue
     * @param runnableWrapper runnable wrapper
     * @param threadFactory thread factory
     * @param handler rejected execution handler
     * @param contextHolder context holder
     * @param ewmaAlpha the alpha parameter for exponentially weighted moving average (a smaller alpha means
     * that new data points will have less weight, where a high alpha means older data points will
     * have a lower influence)
     */
    QueueResizableOpenSearchThreadPoolExecutor(
        String name,
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        ResizableBlockingQueue<Runnable> workQueue,
        Function<Runnable, WrappedRunnable> runnableWrapper,
        ThreadFactory threadFactory,
        XRejectedExecutionHandler handler,
        ThreadContext contextHolder,
        double ewmaAlpha
    ) {
        super(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler, contextHolder);
        this.workQueue = workQueue;
        this.runnableWrapper = runnableWrapper;
        this.executionEWMA = new ExponentiallyWeightedMovingAverage(ewmaAlpha, 0);
        this.poolWaitTime = new CounterMetric();
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
        poolWaitTime.inc(timedRunnable.getWaitTimeNanos());
    }

    /**
     * Resizes the work queue capacity of the pool
     * @param capacity the new capacity
     */
    public synchronized int resize(int capacity) {
        final ResizableBlockingQueue<Runnable> resizableWorkQueue = (ResizableBlockingQueue<Runnable>) workQueue;
        final int currentCapacity = resizableWorkQueue.capacity();
        // Reusing adjustCapacity method instead of introducing the new one
        return resizableWorkQueue.adjustCapacity(
            currentCapacity < capacity ? capacity + 1 : capacity - 1,
            StrictMath.abs(capacity - currentCapacity),
            capacity,
            capacity
        );
    }

    @Override
    public long getPoolWaitTimeNanos() {
        return poolWaitTime.count();
    }
}
