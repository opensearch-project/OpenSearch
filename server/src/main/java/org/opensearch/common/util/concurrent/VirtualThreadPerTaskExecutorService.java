/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;

/**
 * An {@link ExecutorService} that runs each task on its own virtual thread, preserving the {@link ThreadContext} at
 * submission time.
 *
 * @opensearch.internal
 */
public class VirtualThreadPerTaskExecutorService extends ContextPreservingExecutorService {

    /**
     * Tasks submitted but not yet finished. A thread-per-task executor has no queue, so a submitted task is either
     * running or about to be, making this both the in-flight task count and the live thread count.
     */
    private final LongAdder active = new LongAdder();

    /** Tasks that have finished, whether normally or by throwing. */
    private final LongAdder completed = new LongAdder();

    VirtualThreadPerTaskExecutorService(ExecutorService delegate, ThreadContext threadContext) {
        super(delegate, threadContext);
    }

    @Override
    public void execute(Runnable command) {
        active.increment();
        boolean submitted = false;
        try {
            super.execute(command);
            submitted = true;
        } finally {
            if (submitted == false) {
                // the delegate rejected the task (or was already shut down), so it will never run
                active.decrement();
            }
        }
    }

    @Override
    protected void onTaskFinished() {
        active.decrement();
        completed.increment();
    }

    /**
     * Returns the number of tasks that have been submitted but have not yet finished.
     */
    public int getActiveCount() {
        // Clamp to zero because LongAdder::sum isn't atomic when concurrent updates
        // happen but is good enough for stats.
        // Do a simple cast to int because if active count exceeds max int we have
        // bigger problems than this integer overflow.
        return (int) Math.max(0, active.sum());
    }

    /**
     * Returns the number of tasks that have finished executing.
     */
    public long getCompletedTaskCount() {
        return completed.sum();
    }
}
