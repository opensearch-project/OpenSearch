/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import com.sun.management.ThreadMXBean;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.threadpool.RunnableTaskExecutionListener;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.tasks.ResourceStatsType.WORKER_STATS;

/**
 * Service that helps track resource usage of tasks running on a node.
 */
public class TaskResourceTrackingService implements RunnableTaskExecutionListener {

    public static final Setting<Boolean> TASK_RESOURCE_TRACKING_ENABLED = Setting.boolSetting(
        "task_resource_tracking.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private final ConcurrentMapLong<Task> resourceAwareTasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();
    private volatile boolean taskResourceTrackingEnabled;

    public TaskResourceTrackingService(Settings settings, ClusterSettings clusterSettings) {
        this.taskResourceTrackingEnabled = TASK_RESOURCE_TRACKING_ENABLED.get(settings);

        clusterSettings.addSettingsUpdateConsumer(TASK_RESOURCE_TRACKING_ENABLED, this::setTaskResourceTrackingEnabled);
    }

    public boolean isTaskResourceTrackingEnabled() {
        return taskResourceTrackingEnabled
            && threadMXBean.isThreadAllocatedMemorySupported()
            && threadMXBean.isThreadAllocatedMemoryEnabled();
    }

    public void registerTask(Task task) {
        if (isTaskResourceTrackingEnabled()) {
            resourceAwareTasks.put(task.getId(), task);
        }
    }

    public void unregisterTask(Task task) {
        try {
            taskExecutionFinishedOnThread(task.getId(), Thread.currentThread().getId());
        } catch (Exception ignored) {} finally {
            resourceAwareTasks.remove(task.getId(), task);
        }
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
}
