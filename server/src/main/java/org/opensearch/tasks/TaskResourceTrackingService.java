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
import org.opensearch.Version;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.WriteableBase64;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageInfo;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.tasks.resourcetracker.ThreadResourceInfo;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.RunnableTaskExecutionListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.tasks.resourcetracker.ResourceStatsType.WORKER_STATS;

/**
 * Service that helps track resource usage of tasks running on a node.
 */
@SuppressForbidden(reason = "ThreadMXBean#getThreadAllocatedBytes")
public class TaskResourceTrackingService implements RunnableTaskExecutionListener {

    private static final Logger logger = LogManager.getLogger(TaskResourceTrackingService.class);

    public static final Setting<Boolean> TASK_RESOURCE_TRACKING_ENABLED = Setting.boolSetting(
        "task_resource_tracking.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    // Kill-switch for emitting the Base64 binary TASK_RESOURCE_USAGE response header. When false,
    // data nodes always emit the legacy JSON header. The consumer always falls back to JSON regardless.
    public static final Setting<Boolean> BINARY_RESOURCE_USAGE_HEADER_ENABLED = Setting.boolSetting(
        "task_resource_tracking.binary_header.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final String TASK_ID = "TASK_ID";
    public static final String TASK_RESOURCE_USAGE = "TASK_RESOURCE_USAGE";

    // First OpenSearch version that emits and parses the Base64 binary form of the
    // TASK_RESOURCE_USAGE response header. Coordinators older than this only understand JSON.
    static final Version BINARY_RESOURCE_USAGE_HEADER_VERSION = Version.V_3_8_0;

    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private final ConcurrentMapLong<Task> resourceAwareTasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();
    private final List<TaskCompletionListener> taskCompletionListeners = new ArrayList<>();
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private volatile boolean taskResourceTrackingEnabled;
    private volatile boolean binaryResourceUsageHeaderEnabled;

    @Inject
    public TaskResourceTrackingService(
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        this.taskResourceTrackingEnabled = TASK_RESOURCE_TRACKING_ENABLED.get(settings);
        this.binaryResourceUsageHeaderEnabled = BINARY_RESOURCE_USAGE_HEADER_ENABLED.get(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        clusterSettings.addSettingsUpdateConsumer(TASK_RESOURCE_TRACKING_ENABLED, this::setTaskResourceTrackingEnabled);
        clusterSettings.addSettingsUpdateConsumer(BINARY_RESOURCE_USAGE_HEADER_ENABLED, this::setBinaryResourceUsageHeaderEnabled);
    }

    // Without ClusterService, writeTaskResourceUsage cannot verify the coordinator's version
    // and will always emit the legacy JSON header. Forgoes the binary CPU win but is rolling-upgrade safe.
    public TaskResourceTrackingService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this(settings, clusterSettings, threadPool, null);
    }

    public void setTaskResourceTrackingEnabled(boolean taskResourceTrackingEnabled) {
        this.taskResourceTrackingEnabled = taskResourceTrackingEnabled;
    }

    public boolean isTaskResourceTrackingEnabled() {
        return taskResourceTrackingEnabled;
    }

    public void setBinaryResourceUsageHeaderEnabled(boolean binaryResourceUsageHeaderEnabled) {
        this.binaryResourceUsageHeaderEnabled = binaryResourceUsageHeaderEnabled;
    }

    public boolean isBinaryResourceUsageHeaderEnabled() {
        return binaryResourceUsageHeaderEnabled;
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
                taskExecutionFinishedOnThread(task.getId(), Thread.currentThread().threadId());
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

    private ResourceUsageMetric[] getResourceUsageMetricsForThread(long threadId) {
        ResourceUsageMetric currentMemoryUsage = new ResourceUsageMetric(
            ResourceStats.MEMORY,
            threadMXBean.getThreadAllocatedBytes(threadId)
        );
        ResourceUsageMetric currentCPUUsage = new ResourceUsageMetric(ResourceStats.CPU, threadMXBean.getThreadCpuTime(threadId));
        return new ResourceUsageMetric[] { currentMemoryUsage, currentCPUUsage };
    }

    private boolean isCurrentThreadWorkingOnTask(Task task) {
        long threadId = Thread.currentThread().threadId();
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
     * Get the current task level resource usage.
     *
     * @param task {@link SearchShardTask}
     * @param nodeId the local nodeId
     */
    public void writeTaskResourceUsage(SearchShardTask task, String nodeId) {
        try {
            // Get resource usages from when the task started
            ThreadResourceInfo threadResourceInfo = task.getActiveThreadResourceInfo(
                Thread.currentThread().threadId(),
                ResourceStatsType.WORKER_STATS
            );
            if (threadResourceInfo == null) {
                return;
            }
            Map<ResourceStats, ResourceUsageInfo.ResourceStatsInfo> startValues = threadResourceInfo.getResourceUsageInfo().getStatsInfo();
            if (!(startValues.containsKey(ResourceStats.CPU) && startValues.containsKey(ResourceStats.MEMORY))) {
                return;
            }
            // Get current resource usages
            ResourceUsageMetric[] endValues = getResourceUsageMetricsForThread(Thread.currentThread().threadId());
            long cpu = -1, mem = -1;
            for (ResourceUsageMetric endValue : endValues) {
                if (endValue.getStats() == ResourceStats.MEMORY) {
                    mem = endValue.getValue();
                } else if (endValue.getStats() == ResourceStats.CPU) {
                    cpu = endValue.getValue();
                }
            }
            if (cpu == -1 || mem == -1) {
                logger.debug("Invalid resource usage value, cpu [{}], memory [{}]: ", cpu, mem);
                return;
            }

            // Build task resource usage info
            TaskResourceInfo taskResourceInfo = new TaskResourceInfo.Builder().setAction(task.getAction())
                .setTaskId(task.getId())
                .setParentTaskId(task.getParentTaskId().getId())
                .setNodeId(nodeId)
                .setTaskResourceUsage(
                    new TaskResourceUsage(
                        cpu - startValues.get(ResourceStats.CPU).getStartValue(),
                        mem - startValues.get(ResourceStats.MEMORY).getStartValue()
                    )
                )
                .build();
            String headerValue = canCoordinatorReadBinaryHeader(task)
                ? WriteableBase64.encode(taskResourceInfo)
                : taskResourceInfo.toString();
            // Remove the existing TASK_RESOURCE_USAGE header since it would have come from an earlier phase in the same request.
            threadPool.getThreadContext().updateResponseHeader(TASK_RESOURCE_USAGE, headerValue);
        } catch (Exception e) {
            logger.debug("Error during writing task resource usage: ", e);
        }
    }

    // Returns true only with positive confirmation that the coordinator can parse the binary header.
    // Any unknown — kill-switch off, no ClusterService, empty parent task, coordinator missing from
    // cluster state — falls back to JSON, since the cost of an unwarranted JSON emission is small and
    // bounded, while the cost of an unwarranted binary emission to an old coordinator is a hung search.
    private boolean canCoordinatorReadBinaryHeader(SearchShardTask task) {
        if (isBinaryResourceUsageHeaderEnabled() == false) {
            return false;
        }
        if (clusterService == null) {
            return false;
        }
        String coordinatorNodeId = task.getParentTaskId().getNodeId();
        if (coordinatorNodeId.isEmpty()) {
            return false;
        }
        DiscoveryNode coordinator = clusterService.state().nodes().get(coordinatorNodeId);
        if (coordinator == null) {
            return false;
        }
        return coordinator.getVersion().onOrAfter(BINARY_RESOURCE_USAGE_HEADER_VERSION);
    }

    /**
     * Remove the current task level resource usages.
     */
    public void removeTaskResourceUsage() {
        threadPool.getThreadContext().removeResponseHeader(TASK_RESOURCE_USAGE);
    }

    /**
     * Get the task resource usages from {@link ThreadContext}.
     * <p>
     * Tries the new Base64 binary format first; on failure falls back to the legacy JSON form so
     * that resource usage from older data nodes is preserved during a rolling upgrade.
     *
     * @return {@link TaskResourceInfo} or null if the header is absent or cannot be deserialized
     */
    public TaskResourceInfo getTaskResourceUsageFromThreadContext() {
        List<String> taskResourceUsages = threadPool.getThreadContext().getResponseHeaders().get(TASK_RESOURCE_USAGE);
        if (taskResourceUsages == null || taskResourceUsages.isEmpty()) {
            return null;
        }
        String usage = taskResourceUsages.get(0);
        if (usage == null || usage.isEmpty()) {
            return null;
        }
        try {
            return WriteableBase64.decode(usage, TaskResourceInfo::readFromStream);
        } catch (Exception binaryFailure) {
            try {
                return deserializeFromJson(usage);
            } catch (Exception jsonFailure) {
                logger.debug("failed to parse task resource usage header (binary and JSON), skipping: ", jsonFailure);
                return null;
            }
        }
    }

    // Legacy JSON parser for headers emitted by pre-V_3_8_0 data nodes during rolling upgrade.
    static TaskResourceInfo deserializeFromJson(String headerValue) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray(headerValue),
                MediaTypeRegistry.JSON
            )
        ) {
            return TaskResourceInfo.PARSER.apply(parser, null);
        }
    }

    /**
     * Listener that gets invoked when a task execution completes.
     */
    public interface TaskCompletionListener {
        void onTaskCompleted(Task task);
    }

    public void addTaskCompletionListener(TaskCompletionListener listener) {
        this.taskCompletionListeners.add(listener);
    }
}
