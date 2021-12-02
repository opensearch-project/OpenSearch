/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import java.lang.management.ManagementFactory;
import java.util.Objects;

import com.sun.management.ThreadMXBean;
import org.opensearch.ExceptionsHelper;
import org.opensearch.tasks.TaskResourceTracker;

public class ResourceRunnable extends AbstractRunnable implements WrappedRunnable {

    private Runnable original;
    private ThreadContext threadContext;
    ThreadMXBean threadMXBean;
    private String taskId;
    private String threadpoolName;

    public ResourceRunnable(ThreadContext threadContext, final Runnable original, String name) {
        this.original = original;
        this.threadContext = threadContext;
        this.threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        this.threadpoolName = name;
    }

    @Override
    public void onFailure(Exception e) {
        if (original instanceof AbstractRunnable) {
            ((AbstractRunnable) original).onRejection(e);
        } else {
            ExceptionsHelper.reThrowIfNotNull(e);
        }
    }

    @Override
    protected void doRun() throws Exception {
        if (Objects.nonNull(threadContext.getTransient("TASK_ID"))) {
            String taskId = threadContext.getTransient("TASK_ID");
            long threadId = Thread.currentThread().getId();
            TaskResourceTracker.getInstance().registerWorkerForTask(Long.parseLong(taskId), threadId,
                    threadMXBean.getCurrentThreadCpuTime(),
                    threadMXBean.getThreadAllocatedBytes(threadId), threadpoolName);
        }
        try {
            original.run();
        } finally {
            if (Objects.nonNull(threadContext.getTransient("TASK_ID"))) {
                String taskId = threadContext.getTransient("TASK_ID");
                long threadId = Thread.currentThread().getId();
                TaskResourceTracker.getInstance().unregisterWorkerForTask(Long.parseLong(taskId), threadId,
                        threadMXBean.getCurrentThreadCpuTime(), threadMXBean.getThreadAllocatedBytes(threadId));
            }
        }

    }

    @Override
    public Runnable unwrap() {
        return original;
    }
}
