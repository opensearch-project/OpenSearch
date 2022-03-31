/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.WrappedRunnable;

import java.util.Objects;

import static java.lang.Thread.currentThread;
import static org.opensearch.tasks.TaskManager.TASK_ID;

/**
 * Responsible for wrapping the original task's runnable and sending updates on when it starts and finishes to
 * entities listening to the events.
 *
 * It's able to associate runnable with a task with the help of task Id available in thread context.
 */
public class TaskAwareRunnable extends AbstractRunnable implements WrappedRunnable {

    private final Runnable original;
    private final ThreadContext threadContext;
    private final RunnableTaskListenerFactory runnableTaskListener;

    public TaskAwareRunnable(ThreadContext threadContext, final Runnable original, final RunnableTaskListenerFactory runnableTaskListener) {
        this.original = original;
        this.threadContext = threadContext;
        this.runnableTaskListener = runnableTaskListener;
    }

    @Override
    public void onFailure(Exception e) {
        ExceptionsHelper.reThrowIfNotNull(e);
    }

    @Override
    public boolean isForceExecution() {
        return original instanceof AbstractRunnable && ((AbstractRunnable) original).isForceExecution();
    }

    @Override
    public void onRejection(final Exception e) {
        if (original instanceof AbstractRunnable) {
            ((AbstractRunnable) original).onRejection(e);
        } else {
            ExceptionsHelper.reThrowIfNotNull(e);
        }
    }

    @Override
    protected void doRun() throws Exception {
        assert runnableTaskListener.get() != null : "Listener should be attached";

        Long taskId = threadContext.getTransient(TASK_ID);

        if (Objects.nonNull(taskId)) {
            runnableTaskListener.get().taskExecutionStartedOnThread(taskId, currentThread().getId());
        }
        try {
            original.run();
        } finally {
            if (Objects.nonNull(taskId)) {
                runnableTaskListener.get().taskExecutionFinishedOnThread(taskId, currentThread().getId());
            }
        }

    }

    @Override
    public Runnable unwrap() {
        return original;
    }
}
