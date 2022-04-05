/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import com.sun.management.ThreadMXBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.RunnableTaskExecutionListener;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.tasks.ResourceStatsType.WORKER_STATS;

/**
 * Service that helps track resource usage of tasks running on a node.
 */
public class TaskResourceTrackingService implements RunnableTaskExecutionListener {

    private static final Logger logger = LogManager.getLogger(TaskManager.class);

    public static final Setting<Boolean> TASK_RESOURCE_TRACKING_ENABLED = Setting.boolSetting(
        "task_resource_tracking.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final String TASK_ID = "TASK_ID";

    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    public final ConcurrentMapLong<Task> resourceAwareTasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();
    private volatile boolean taskResourceTrackingEnabled;
    private ThreadPool threadPool;

    public TaskResourceTrackingService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.taskResourceTrackingEnabled = TASK_RESOURCE_TRACKING_ENABLED.get(settings);
        this.threadPool = threadPool;

        clusterSettings.addSettingsUpdateConsumer(TASK_RESOURCE_TRACKING_ENABLED, this::setTaskResourceTrackingEnabled);
    }

    public boolean isTaskResourceTrackingEnabled() {
        return taskResourceTrackingEnabled
            && threadMXBean.isThreadAllocatedMemorySupported()
            && threadMXBean.isThreadAllocatedMemoryEnabled();
    }

    public ThreadContext.StoredContext startTracking(Task task) {
        if (task.supportsResourceTracking() == false || isTaskResourceTrackingEnabled() == false) {
            return () -> {};
        }

        resourceAwareTasks.put(task.getId(), task);
        return addTaskIdToThreadContext(task);

    }

    /**
     * unregisters tasks registered earlier.
     * <p>
     * It doesn't have feature enabled check to avoid any issues if setting was disable while the task was in progress.
     * <p>
     * It's also responsible to stop tracking the current thread's resources against this task if not already done.
     * This happens when the thread handling the request itself calls the unregister method. So in this case unregister
     * happens before runnable finishes.
     *
     * @param task
     */
    public void stopTracking(Task task) {
        if (isThreadWorkingOnTask(task, Thread.currentThread().getId())) {
            taskExecutionFinishedOnThread(task.getId(), Thread.currentThread().getId());
        }

        assert validateNoActiveThread(task) : "No thread should be active when task is finished";

        resourceAwareTasks.remove(task.getId());
    }

    private boolean validateNoActiveThread(Task task) {
        for (List<ThreadResourceInfo> threadResourceInfos : task.getResourceStats().values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfos) {
                if (threadResourceInfo.isActive()) return false;
            }
        }
        return true;
    }

    public void refreshResourceStats(Task... tasks) {
        if (isTaskResourceTrackingEnabled() == false) {
            return;
        }

        for (Task task : tasks) {
            if (task.supportsResourceTracking() && resourceAwareTasks.containsKey(task.getId())) {
                refreshResourceStats(task);
            }
        }
    }

    private void refreshResourceStats(Task resourceAwareTask) {
        resourceAwareTask.getResourceStats().forEach((threadId, threadResourceInfos) -> {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfos) {
                if (threadResourceInfo.isActive()) {
                    resourceAwareTask.updateThreadResourceStats(threadId, WORKER_STATS, getResourceUsageMetricsForThread(threadId));
                }
            }
        });
    }

    public void setTaskResourceTrackingEnabled(boolean taskResourceTrackingEnabled) {
        this.taskResourceTrackingEnabled = taskResourceTrackingEnabled;
    }

    /**
     * Called when a thread starts working on a task's runnable.
     *
     * @param taskId   of the task for which runnable is starting
     * @param threadId of the thread which will be executing the runnable and we need to check resource usage for this
     *                 thread
     */
    @Override
    public void taskExecutionStartedOnThread(long taskId, long threadId) {
        if (resourceAwareTasks.containsKey(taskId)) {
            resourceAwareTasks.get(taskId).startThreadResourceTracking(threadId, WORKER_STATS, getResourceUsageMetricsForThread(threadId));
        }
    }

    /**
     * Called when a thread finishes working on a task's runnable.
     *
     * @param taskId   of the task for which runnable is complete
     * @param threadId of the thread which executed the runnable and we need to check resource usage for this thread
     */
    @Override
    public void taskExecutionFinishedOnThread(long taskId, long threadId) {
        if (resourceAwareTasks.containsKey(taskId)) {
            resourceAwareTasks.get(taskId).stopThreadResourceTracking(threadId, WORKER_STATS, getResourceUsageMetricsForThread(threadId));
        }
    }

    public Map<Long, Task> getResourceAwareTasks() {
        return Collections.unmodifiableMap(resourceAwareTasks);
    }

    private ResourceUsageMetric[] getResourceUsageMetricsForThread(long threadId) {
        ResourceUsageMetric currentMemoryUsage = new ResourceUsageMetric(
            ResourceStats.MEMORY,
            threadMXBean.getThreadAllocatedBytes(threadId)
        );
        ResourceUsageMetric currentCPUUsage = new ResourceUsageMetric(ResourceStats.CPU, threadMXBean.getThreadCpuTime(threadId));
        return new ResourceUsageMetric[] { currentMemoryUsage, currentCPUUsage };
    }

    private boolean isThreadWorkingOnTask(Task task, long threadId) {
        List<ThreadResourceInfo> threadResourceInfos = task.getResourceStats().getOrDefault(threadId, Collections.emptyList());

        for (ThreadResourceInfo threadResourceInfo : threadResourceInfos) {
            if (threadResourceInfo.isActive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds Task Id in the ThreadContext.
     * <p>
     * Stashes the existing ThreadContext and preserves all the existing ThreadContext's data in the new ThreadContext
     * as well.
     *
     * @param task for which Task Id needs to be added in ThreadContext.
     * @return StoredContext reference to restore the ThreadContext from which we created a new one.
     * Caller can call context.restore() to get the existing ThreadContext back.
     */
    private ThreadContext.StoredContext addTaskIdToThreadContext(Task task) {
        ThreadContext threadContext = threadPool.getThreadContext();

        boolean noStaleTaskIdPresentInThreadContext = threadContext.getTransient(TASK_ID) == null
            || resourceAwareTasks.containsKey((long) threadContext.getTransient(TASK_ID));
        assert noStaleTaskIdPresentInThreadContext : "Stale Task Id shouldn't be present in thread context";

        ThreadContext.StoredContext storedContext = threadContext.newStoredContext(true, Collections.singletonList(TASK_ID));
        threadContext.putTransient(TASK_ID, task.getId());
        return storedContext;
    }

}
