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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.core.tasks.resourcetracker.ThreadResourceInfo;
import org.opensearch.threadpool.RunnableTaskExecutionListener;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.tasks.resourcetracker.ResourceStatsType.WORKER_STATS;

/**
 * Service that helps track resource usage of tasks running on a node.
 */
@PublicApi(since = "2.2.0")
@SuppressForbidden(reason = "ThreadMXBean#getThreadAllocatedBytes")
public class TaskResourceTrackingService implements RunnableTaskExecutionListener {

    private static final Logger logger = LogManager.getLogger(TaskManager.class);

    public static final Setting<Boolean> TASK_RESOURCE_TRACKING_ENABLED = Setting.boolSetting(
        "task_resource_tracking.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final String TASK_ID = "TASK_ID";

    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private final ConcurrentMapLong<Task> resourceAwareTasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();
    private final List<TaskCompletionListener> taskCompletionListeners = new ArrayList<>();
    private final List<TaskStartListener> taskStartListeners = new ArrayList<>();
    private final ThreadPool threadPool;
    private volatile boolean taskResourceTrackingEnabled;

    @Inject
    public TaskResourceTrackingService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.taskResourceTrackingEnabled = TASK_RESOURCE_TRACKING_ENABLED.get(settings);
        this.threadPool = threadPool;
        clusterSettings.addSettingsUpdateConsumer(TASK_RESOURCE_TRACKING_ENABLED, this::setTaskResourceTrackingEnabled);
    }

    public void setTaskResourceTrackingEnabled(boolean taskResourceTrackingEnabled) {
        this.taskResourceTrackingEnabled = taskResourceTrackingEnabled;
    }

    public boolean isTaskResourceTrackingEnabled() {
        return taskResourceTrackingEnabled;
    }

    public boolean isTaskResourceTrackingSupported() {
        return threadMXBean.isThreadAllocatedMemorySupported() && threadMXBean.isThreadAllocatedMemoryEnabled();
    }

    /**
     * Executes logic only if task supports resource tracking and resource tracking setting is enabled.
     * <p>
     * 1. Starts tracking the task in map of resourceAwareTasks.
     * 2. Adds Task Id in thread context to make sure it's available while task is processed across multiple threads.
     *
     * @param task for which resources needs to be tracked
     * @return Autocloseable stored context to restore ThreadContext to the state before this method changed it.
     */
    public ThreadContext.StoredContext startTracking(Task task) {
        if (task.supportsResourceTracking() == false
            || isTaskResourceTrackingEnabled() == false
            || isTaskResourceTrackingSupported() == false) {
            return () -> {};
        }

        logger.debug("Starting resource tracking for task: {}", task.getId());
        resourceAwareTasks.put(task.getId(), task);

        List<Exception> exceptions = new ArrayList<>();
        for (TaskStartListener listener : taskStartListeners) {
            try {
                listener.onTaskStarts(task);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);

        return addTaskIdToThreadContext(task);
    }

    /**
     * Stops tracking task registered earlier for tracking.
     * <p>
     * It doesn't have feature enabled check to avoid any issues if setting was disable while the task was in progress.
     * <p>
     * It's also responsible to stop tracking the current thread's resources against this task if not already done.
     * This happens when the thread executing the request logic itself calls the unregister method. So in this case unregister
     * happens before runnable finishes.
     *
     * @param task task which has finished and doesn't need resource tracking.
     */
    public void stopTracking(Task task) {
        logger.debug("Stopping resource tracking for task: {}", task.getId());
        try {
            if (isCurrentThreadWorkingOnTask(task)) {
                taskExecutionFinishedOnThread(task.getId(), Thread.currentThread().getId());
            }
        } catch (Exception e) {
            logger.warn("Failed while trying to mark the task execution on current thread completed.", e);
            assert false;
        } finally {
            resourceAwareTasks.remove(task.getId());
        }

        List<Exception> exceptions = new ArrayList<>();
        for (TaskCompletionListener listener : taskCompletionListeners) {
            try {
                listener.onTaskCompleted(task);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    /**
     * Refreshes the resource stats for the tasks provided by looking into which threads are actively working on these
     * and how much resources these have consumed till now.
     *
     * @param tasks for which resource stats needs to be refreshed.
     */
    public void refreshResourceStats(Task... tasks) {
        if (isTaskResourceTrackingEnabled() == false || isTaskResourceTrackingSupported() == false) {
            return;
        }

        for (Task task : tasks) {
            if (task.supportsResourceTracking() && resourceAwareTasks.containsKey(task.getId())) {
                refreshResourceStats(task);
            }
        }
    }

    private void refreshResourceStats(Task resourceAwareTask) {
        try {
            logger.debug("Refreshing resource stats for Task: {}", resourceAwareTask.getId());
            List<Long> threadsWorkingOnTask = getThreadsWorkingOnTask(resourceAwareTask);
            threadsWorkingOnTask.forEach(
                threadId -> resourceAwareTask.updateThreadResourceStats(threadId, WORKER_STATS, getResourceUsageMetricsForThread(threadId))
            );
        } catch (IllegalStateException e) {
            logger.debug("Resource stats already updated.");
        }

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
        try {
            final Task task = resourceAwareTasks.get(taskId);
            if (task != null) {
                logger.debug("Task execution started on thread. Task: {}, Thread: {}", taskId, threadId);
                task.startThreadResourceTracking(threadId, WORKER_STATS, getResourceUsageMetricsForThread(threadId));
            }
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("Failed to mark thread execution started for task: [{}]", taskId), e);
            assert false;
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
        try {
            final Task task = resourceAwareTasks.get(taskId);
            if (task != null) {
                logger.debug("Task execution finished on thread. Task: {}, Thread: {}", taskId, threadId);
                task.stopThreadResourceTracking(threadId, WORKER_STATS, getResourceUsageMetricsForThread(threadId));
            }
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("Failed to mark thread execution finished for task: [{}]", taskId), e);
            assert false;
        }
    }

    public Map<Long, Task> getResourceAwareTasks() {
        return Collections.unmodifiableMap(resourceAwareTasks);
    }

    public static ResourceUsageMetric[] getResourceUsageMetricsForThread(long threadId) {
        ResourceUsageMetric currentMemoryUsage = new ResourceUsageMetric(
            ResourceStats.MEMORY,
            threadMXBean.getThreadAllocatedBytes(threadId)
        );
        ResourceUsageMetric currentCPUUsage = new ResourceUsageMetric(ResourceStats.CPU, threadMXBean.getThreadCpuTime(threadId));
        return new ResourceUsageMetric[] { currentMemoryUsage, currentCPUUsage };
    }

    private boolean isCurrentThreadWorkingOnTask(Task task) {
        long threadId = Thread.currentThread().getId();
        List<ThreadResourceInfo> threadResourceInfos = task.getResourceStats().getOrDefault(threadId, Collections.emptyList());

        for (ThreadResourceInfo threadResourceInfo : threadResourceInfos) {
            if (threadResourceInfo.isActive()) {
                return true;
            }
        }
        return false;
    }

    private List<Long> getThreadsWorkingOnTask(Task task) {
        List<Long> activeThreads = new ArrayList<>();
        for (List<ThreadResourceInfo> threadResourceInfos : task.getResourceStats().values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfos) {
                if (threadResourceInfo.isActive()) {
                    activeThreads.add(threadResourceInfo.getThreadId());
                }
            }
        }
        return activeThreads;
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
        ThreadContext.StoredContext storedContext = threadContext.newStoredContext(true, Collections.singletonList(TASK_ID));
        threadContext.putTransient(TASK_ID, task.getId());
        return storedContext;
    }

    /**
     * Listener that gets invoked when a task execution completes.
     */
    @PublicApi(since = "2.2.0")
    public interface TaskCompletionListener {
        void onTaskCompleted(Task task);
    }

    /**
     * Listener that gets invoked when a task execution starts.
     */
    @PublicApi(since = "2.2.0")
    public interface TaskStartListener {
        void onTaskStarts(Task task);
    }

    public void addTaskCompletionListener(TaskCompletionListener listener) {
        this.taskCompletionListeners.add(listener);
    }

    public void addTaskStartListener(TaskStartListener listener) {
        this.taskStartListeners.add(listener);
    }
}
