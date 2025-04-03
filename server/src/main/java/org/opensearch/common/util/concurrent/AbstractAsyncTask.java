/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.util.concurrent;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.Randomness;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * A base class for tasks that need to repeat.
 *
 * @opensearch.internal
 */
public abstract class AbstractAsyncTask implements Runnable, Closeable {

    private final Logger logger;
    private final ThreadPool threadPool;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean autoReschedule;
    private volatile Scheduler.Cancellable cancellable;
    private volatile boolean isScheduledOrRunning;
    private volatile Exception lastThrownException;
    private volatile TimeValue interval;
    private volatile long lastRunStartTimeNs = -1;
    private final Supplier<Boolean> fixedIntervalSchedulingEnabled;

    protected AbstractAsyncTask(Logger logger, ThreadPool threadPool, TimeValue interval, boolean autoReschedule) {
        this(logger, threadPool, interval, autoReschedule, () -> Boolean.FALSE);
    }

    protected AbstractAsyncTask(
        Logger logger,
        ThreadPool threadPool,
        TimeValue interval,
        boolean autoReschedule,
        Supplier<Boolean> fixedIntervalSchedulingEnabled
    ) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.interval = interval;
        this.autoReschedule = autoReschedule;
        this.fixedIntervalSchedulingEnabled = fixedIntervalSchedulingEnabled;
    }

    /**
     * Change the interval between runs.
     * If a future run is scheduled then this will reschedule it.
     *
     * @param interval The new interval between runs.
     */
    public synchronized void setInterval(TimeValue interval) {
        this.interval = interval;
        if (cancellable != null) {
            rescheduleIfNecessary();
        }
    }

    public TimeValue getInterval() {
        return interval;
    }

    /**
     * Test any external conditions that determine whether the task
     * should be scheduled.  This method does *not* need to test if
     * the task is closed, as being closed automatically prevents
     * scheduling.
     *
     * @return Should the task be scheduled to run?
     */
    protected abstract boolean mustReschedule();

    /**
     * Schedule the task to run after the configured interval if it
     * is not closed and any further conditions imposed by derived
     * classes are met.  Any previously scheduled invocation is
     * cancelled.
     */
    public synchronized void rescheduleIfNecessary() {
        if (isClosed()) {
            return;
        }
        if (cancellable != null) {
            cancellable.cancel();
        }
        if (interval.millis() > 0 && mustReschedule()) {
            if (logger.isTraceEnabled()) {
                logger.trace("scheduling {} every {}", toString(), interval);
            }
            cancellable = threadPool.schedule(this, getSleepDuration(), getThreadPool());
            isScheduledOrRunning = true;
        } else {
            logger.trace("scheduled {} disabled", toString());
            cancellable = null;
            isScheduledOrRunning = false;
        }
    }

    public boolean isScheduled() {
        // Currently running counts as scheduled to avoid an oscillating return value
        // from this method when a task is repeatedly running and rescheduling itself.
        return isScheduledOrRunning;
    }

    /**
     * Cancel any scheduled run, but do not prevent subsequent restarts.
     */
    public synchronized void cancel() {
        if (cancellable != null) {
            cancellable.cancel();
            cancellable = null;
        }
        isScheduledOrRunning = false;
    }

    /**
     * Cancel any scheduled run
     */
    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            cancel();
        }
    }

    public boolean isClosed() {
        return this.closed.get();
    }

    @Override
    public final void run() {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            cancellable = null;
            isScheduledOrRunning = autoReschedule;
        }
        try {
            lastRunStartTimeNs = System.nanoTime();
            runInternal();
        } catch (Exception ex) {
            if (lastThrownException == null || sameException(lastThrownException, ex) == false) {
                // prevent the annoying fact of logging the same stuff all the time with an interval of 1 sec will spam all your logs
                logger.warn(
                    () -> new ParameterizedMessage(
                        "failed to run task {} - suppressing re-occurring exceptions unless the exception changes",
                        toString()
                    ),
                    ex
                );
                lastThrownException = ex;
            }
        } finally {
            if (autoReschedule) {
                rescheduleIfNecessary();
            }
        }
    }

    private static boolean sameException(Exception left, Exception right) {
        if (left.getClass() == right.getClass()) {
            if (Objects.equals(left.getMessage(), right.getMessage())) {
                StackTraceElement[] stackTraceLeft = left.getStackTrace();
                StackTraceElement[] stackTraceRight = right.getStackTrace();
                if (stackTraceLeft.length == stackTraceRight.length) {
                    for (int i = 0; i < stackTraceLeft.length; i++) {
                        if (stackTraceLeft[i].equals(stackTraceRight[i]) == false) {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    protected abstract void runInternal();

    /**
     * Use the same threadpool by default.
     * Derived classes can change this if required.
     */
    protected String getThreadPool() {
        return ThreadPool.Names.SAME;
    }

    /**
     * Calculates the sleep duration for the next scheduled execution of the task.
     * This method determines the appropriate delay based on the last run time and the configured interval
     * to schedule the next execution.
     */
    public TimeValue getSleepDuration() {
        if (!fixedIntervalSchedulingEnabled.get()) {
            return interval;
        }

        if (lastRunStartTimeNs == -1) {
            // We want to stagger the start of refreshes in random manner so that we avoid refreshes to happen at the same
            // when we have refreshes happening in parallel for multiple shards of the same index. a.k.a. Dense shard packing
            long sleepTimeNs = Randomness.get().nextLong(interval.nanos());
            return TimeValue.timeValueNanos(sleepTimeNs);
        }

        long timeSinceLastRunNs = System.nanoTime() - lastRunStartTimeNs;
        if (timeSinceLastRunNs >= interval.nanos()) {
            // If the time taken for refresh is more than the configured refresh interval, then we schedule the next refresh
            // immediately. This is to avoid the case where the time taken for refresh is more than the configured refresh
            // interval due to the processing of the refresh request.
            return TimeValue.ZERO;
        } else {
            // If the time taken for refresh is less than the configured refresh interval, then we schedule the next refresh
            // after the remaining time for the refresh interval.
            return TimeValue.timeValueNanos(interval.nanos() - timeSinceLastRunNs);
        }
    }
}
