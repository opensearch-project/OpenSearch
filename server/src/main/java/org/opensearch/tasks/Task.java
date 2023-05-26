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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.NotifyOnceListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Current task information
 *
 * @opensearch.internal
 */
public class Task {

    private static final Logger logger = LogManager.getLogger(Task.class);

    /**
     * The request header to mark tasks with specific ids
     */
    public static final String X_OPAQUE_ID = "X-Opaque-Id";

    private static final String TOTAL = "total";

    private static final String AVERAGE = "average";

    private static final String MIN = "min";

    private static final String MAX = "max";

    public static final String THREAD_INFO = "thread_info";

    private final long id;

    private final String type;

    private final String action;

    private final String description;

    private final TaskId parentTask;

    private final Map<String, String> headers;

    private final Map<Long, List<ThreadResourceInfo>> resourceStats;

    private final List<NotifyOnceListener<Task>> resourceTrackingCompletionListeners;

    /**
     * Keeps track of the number of active resource tracking threads for this task. It is initialized to 1 to track
     * the task's own/self thread. When this value becomes 0, all threads have been marked inactive and the resource
     * tracking can be stopped for this task.
     */
    private final AtomicInteger numActiveResourceTrackingThreads = new AtomicInteger(1);

    /**
     * The task's start time as a wall clock time since epoch ({@link System#currentTimeMillis()} style).
     */
    private final long startTime;

    /**
     * The task's start time as a relative time ({@link System#nanoTime()} style).
     */
    private final long startTimeNanos;

    public Task(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        this(
            id,
            type,
            action,
            description,
            parentTask,
            System.currentTimeMillis(),
            System.nanoTime(),
            headers,
            new ConcurrentHashMap<>(),
            new ArrayList<>()
        );
    }

    public Task(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        long startTime,
        long startTimeNanos,
        Map<String, String> headers,
        ConcurrentHashMap<Long, List<ThreadResourceInfo>> resourceStats,
        List<NotifyOnceListener<Task>> resourceTrackingCompletionListeners
    ) {
        this.id = id;
        this.type = type;
        this.action = action;
        this.description = description;
        this.parentTask = parentTask;
        this.startTime = startTime;
        this.startTimeNanos = startTimeNanos;
        this.headers = headers;
        this.resourceStats = resourceStats;
        this.resourceTrackingCompletionListeners = resourceTrackingCompletionListeners;
    }

    /**
     * Build a version of the task status you can throw over the wire and back
     * to the user.
     *
     * @param localNodeId
     *            the id of the node this task is running on
     * @param detailed
     *            should the information include detailed, potentially slow to
     *            generate data?
     */
    public final TaskInfo taskInfo(String localNodeId, boolean detailed) {
        return taskInfo(localNodeId, detailed, detailed == false);
    }

    /**
     * Build a version of the task status you can throw over the wire and back
     * with the option to include resource stats or not.
     * This method is only used during creating TaskResult to avoid storing resource information into the task index.
     *
     * @param excludeStats should information exclude resource stats.
     *                     By default, detailed flag is used to control including resource information.
     *                     But inorder to avoid storing resource stats into task index as strict mapping is enforced and breaks when adding this field.
     *                     In the future, task-index-mapping.json can be modified to add resource stats.
     */
    private TaskInfo taskInfo(String localNodeId, boolean detailed, boolean excludeStats) {
        String description = null;
        Task.Status status = null;
        TaskResourceStats resourceStats = null;
        if (detailed) {
            description = getDescription();
            status = getStatus();
        }
        if (excludeStats == false) {
            resourceStats = new TaskResourceStats(new HashMap<>() {
                {
                    put(TOTAL, getTotalResourceStats());
                    put(AVERAGE, getAverageResourceStats());
                    put(MIN, getMinResourceStats());
                    put(MAX, getMaxResourceStats());
                }
            }, getThreadUsage());
        }
        return taskInfo(localNodeId, description, status, resourceStats);
    }

    /**
     * Build a {@link TaskInfo} for this task without resource stats.
     */
    protected final TaskInfo taskInfo(String localNodeId, String description, Status status) {
        return taskInfo(localNodeId, description, status, null);
    }

    /**
     * Build a proper {@link TaskInfo} for this task.
     */
    protected final TaskInfo taskInfo(String localNodeId, String description, Status status, TaskResourceStats resourceStats) {
        boolean cancelled = this instanceof CancellableTask && ((CancellableTask) this).isCancelled();
        Long cancellationStartTime = null;
        if (cancelled) {
            cancellationStartTime = ((CancellableTask) this).getCancellationStartTime();
        }
        return new TaskInfo(
            new TaskId(localNodeId, getId()),
            getType(),
            getAction(),
            description,
            status,
            startTime,
            System.nanoTime() - startTimeNanos,
            this instanceof CancellableTask,
            cancelled,
            parentTask,
            headers,
            resourceStats,
            cancellationStartTime
        );
    }

    /**
     * Returns task id
     */
    public long getId() {
        return id;
    }

    /**
     * Returns task channel type (netty, transport, direct)
     */
    public String getType() {
        return type;
    }

    /**
     * Returns task action
     */
    public String getAction() {
        return action;
    }

    /**
     * Generates task description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the task's start time as a wall clock time since epoch ({@link System#currentTimeMillis()} style).
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the task's start time in nanoseconds ({@link System#nanoTime()} style).
     */
    public long getStartTimeNanos() {
        return startTimeNanos;
    }

    /**
     * Returns id of the parent task or NO_PARENT_ID if the task doesn't have any parent tasks
     */
    public TaskId getParentTaskId() {
        return parentTask;
    }

    /**
     * Build a status for this task or null if this task doesn't have status.
     * Since most tasks don't have status this defaults to returning null. While
     * this can never perform IO it might be a costly operation, requiring
     * collating lists of results, etc. So only use it if you need the value.
     */
    public Status getStatus() {
        return null;
    }

    /**
     * Returns thread level resource consumption of the task
     */
    public Map<Long, List<ThreadResourceInfo>> getResourceStats() {
        return Collections.unmodifiableMap(resourceStats);
    }

    /**
     * Returns current total resource usage of the task.
     * Currently, this method is only called on demand, during get and listing of tasks.
     * In the future, these values can be cached as an optimization.
     */
    public TaskResourceUsage getTotalResourceStats() {
        return new TaskResourceUsage(getTotalResourceUtilization(ResourceStats.CPU), getTotalResourceUtilization(ResourceStats.MEMORY));
    }

    /**
     * Returns current average per-execution resource usage of the task.
     */
    public TaskResourceUsage getAverageResourceStats() {
        return new TaskResourceUsage(getAverageResourceUtilization(ResourceStats.CPU), getAverageResourceUtilization(ResourceStats.MEMORY));
    }

    /**
     * Returns current min per-execution resource usage of the task.
     */
    public TaskResourceUsage getMinResourceStats() {
        return new TaskResourceUsage(getMinResourceUtilization(ResourceStats.CPU), getMinResourceUtilization(ResourceStats.MEMORY));
    }

    /**
     * Returns current max per-execution resource usage of the task.
     */
    public TaskResourceUsage getMaxResourceStats() {
        return new TaskResourceUsage(getMaxResourceUtilization(ResourceStats.CPU), getMaxResourceUtilization(ResourceStats.MEMORY));
    }

    /**
     * Returns total resource consumption for a specific task stat.
     */
    public long getTotalResourceUtilization(ResourceStats stats) {
        long totalResourceConsumption = 0L;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                final ResourceUsageInfo.ResourceStatsInfo statsInfo = threadResourceInfo.getResourceUsageInfo().getStatsInfo().get(stats);
                if (threadResourceInfo.getStatsType().isOnlyForAnalysis() == false && statsInfo != null) {
                    totalResourceConsumption += statsInfo.getTotalValue();
                }
            }
        }
        return totalResourceConsumption;
    }

    /**
     * Returns average per-execution resource consumption for a specific task stat.
     */
    private long getAverageResourceUtilization(ResourceStats stats) {
        long totalResourceConsumption = 0L;
        int threadResourceInfoCount = 0;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                final ResourceUsageInfo.ResourceStatsInfo statsInfo = threadResourceInfo.getResourceUsageInfo().getStatsInfo().get(stats);
                if (threadResourceInfo.getStatsType().isOnlyForAnalysis() == false && statsInfo != null) {
                    totalResourceConsumption += statsInfo.getTotalValue();
                    threadResourceInfoCount++;
                }
            }
        }
        return (threadResourceInfoCount > 0) ? totalResourceConsumption / threadResourceInfoCount : 0;
    }

    /**
     * Returns minimum per-execution resource consumption for a specific task stat.
     */
    private long getMinResourceUtilization(ResourceStats stats) {
        if (resourceStats.size() == 0) {
            return 0L;
        }
        long minResourceConsumption = Long.MAX_VALUE;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                final ResourceUsageInfo.ResourceStatsInfo statsInfo = threadResourceInfo.getResourceUsageInfo().getStatsInfo().get(stats);
                if (threadResourceInfo.getStatsType().isOnlyForAnalysis() == false && statsInfo != null) {
                    minResourceConsumption = Math.min(minResourceConsumption, statsInfo.getTotalValue());
                }
            }
        }
        return minResourceConsumption;
    }

    /**
     * Returns maximum per-execution resource consumption for a specific task stat.
     */
    private long getMaxResourceUtilization(ResourceStats stats) {
        long maxResourceConsumption = 0L;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                final ResourceUsageInfo.ResourceStatsInfo statsInfo = threadResourceInfo.getResourceUsageInfo().getStatsInfo().get(stats);
                if (threadResourceInfo.getStatsType().isOnlyForAnalysis() == false && statsInfo != null) {
                    maxResourceConsumption = Math.max(maxResourceConsumption, statsInfo.getTotalValue());
                }
            }
        }
        return maxResourceConsumption;
    }

    /**
     * Returns the total and active number of thread executions for the task.
     */
    public TaskThreadUsage getThreadUsage() {
        int numThreadExecutions = 0;
        int activeThreads = 0;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            numThreadExecutions += threadResourceInfosList.size();
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                if (threadResourceInfo.isActive()) {
                    activeThreads++;
                }
            }
        }
        return new TaskThreadUsage(numThreadExecutions, activeThreads);
    }

    /**
     * Adds thread's starting resource consumption information
     * @param threadId ID of the thread
     * @param statsType stats type
     * @param resourceUsageMetrics resource consumption metrics of the thread
     * @throws IllegalStateException matching active thread entry was found which is not expected.
     */
    public void startThreadResourceTracking(long threadId, ResourceStatsType statsType, ResourceUsageMetric... resourceUsageMetrics) {
        final List<ThreadResourceInfo> threadResourceInfoList = resourceStats.computeIfAbsent(threadId, k -> new ArrayList<>());
        // active thread entry should not be present in the list
        for (ThreadResourceInfo threadResourceInfo : threadResourceInfoList) {
            if (threadResourceInfo.getStatsType() == statsType && threadResourceInfo.isActive()) {
                throw new IllegalStateException(
                    "unexpected active thread resource entry present [" + threadId + "]:[" + threadResourceInfo + "]"
                );
            }
        }
        threadResourceInfoList.add(new ThreadResourceInfo(threadId, statsType, resourceUsageMetrics));
        incrementResourceTrackingThreads();
    }

    /**
     * This method is used to update the resource consumption stats so that the data isn't too stale for long-running task.
     * If active thread entry is present in the list, the entry is updated. If one is not found, it throws an exception.
     * @param threadId ID of the thread
     * @param statsType stats type
     * @param resourceUsageMetrics resource consumption metrics of the thread
     * @throws IllegalStateException if no matching active thread entry was found.
     */
    public void updateThreadResourceStats(long threadId, ResourceStatsType statsType, ResourceUsageMetric... resourceUsageMetrics) {
        final List<ThreadResourceInfo> threadResourceInfoList = resourceStats.get(threadId);
        if (threadResourceInfoList != null) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfoList) {
                // the active entry present in the list is updated
                if (threadResourceInfo.getStatsType() == statsType && threadResourceInfo.isActive()) {
                    threadResourceInfo.recordResourceUsageMetrics(resourceUsageMetrics);
                    return;
                }
            }
        }
        throw new IllegalStateException("cannot update if active thread resource entry is not present");
    }

    /**
     * Record the thread's final resource consumption values.
     * If active thread entry is present in the list, the entry is updated. If one is not found, it throws an exception.
     * @param threadId ID of the thread
     * @param statsType stats type
     * @param resourceUsageMetrics resource consumption metrics of the thread
     * @throws IllegalStateException if no matching active thread entry was found.
     */
    public void stopThreadResourceTracking(long threadId, ResourceStatsType statsType, ResourceUsageMetric... resourceUsageMetrics) {
        final List<ThreadResourceInfo> threadResourceInfoList = resourceStats.get(threadId);
        if (threadResourceInfoList != null) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfoList) {
                if (threadResourceInfo.getStatsType() == statsType && threadResourceInfo.isActive()) {
                    threadResourceInfo.setActive(false);
                    threadResourceInfo.recordResourceUsageMetrics(resourceUsageMetrics);
                    decrementResourceTrackingThreads();
                    return;
                }
            }
        }
        throw new IllegalStateException("cannot update final values if active thread resource entry is not present");
    }

    /**
     * Individual tasks can override this if they want to support task resource tracking. We just need to make sure that
     * the ThreadPool on which the task runs on have runnable wrapper similar to
     * {@link org.opensearch.common.util.concurrent.OpenSearchExecutors#newResizable}
     *
     * @return true if resource tracking is supported by the task
     */
    public boolean supportsResourceTracking() {
        return false;
    }

    /**
     * Report of the internal status of a task. These can vary wildly from task
     * to task because each task is implemented differently but we should try
     * to keep each task consistent from version to version where possible.
     * That means each implementation of {@linkplain Task.Status#toXContent}
     * should avoid making backwards incompatible changes to the rendered
     * result. But if we change the way a request is implemented it might not
     * be possible to preserve backwards compatibility. In that case, we
     * <b>can</b> change this on version upgrade but we should be careful
     * because some statuses (reindex) have become defacto standardized because
     * they are used by systems like Kibana.
     */
    public interface Status extends ToXContentObject, NamedWriteable {}

    /**
     * Returns stored task header associated with the task
     */
    public String getHeader(String header) {
        return headers.get(header);
    }

    public TaskResult result(DiscoveryNode node, Exception error) throws IOException {
        return new TaskResult(taskInfo(node.getId(), true, true), error);
    }

    public TaskResult result(DiscoveryNode node, ActionResponse response) throws IOException {
        if (response instanceof ToXContent) {
            return new TaskResult(taskInfo(node.getId(), true, true), (ToXContent) response);
        } else {
            throw new IllegalStateException("response has to implement ToXContent to be able to store the results");
        }
    }

    /**
     * Registers a task resource tracking completion listener on this task if resource tracking is still active.
     * Returns true on successful subscription, false otherwise.
     */
    public boolean addResourceTrackingCompletionListener(NotifyOnceListener<Task> listener) {
        if (numActiveResourceTrackingThreads.get() > 0) {
            resourceTrackingCompletionListeners.add(listener);
            return true;
        }

        return false;
    }

    /**
     * Increments the number of active resource tracking threads.
     *
     * @return the number of active resource tracking threads.
     */
    public int incrementResourceTrackingThreads() {
        return numActiveResourceTrackingThreads.incrementAndGet();
    }

    /**
     * Decrements the number of active resource tracking threads.
     * This method is called when threads finish execution, and also when the task is unregistered (to mark the task's
     * own thread as complete). When the active thread count becomes zero, the onTaskResourceTrackingCompleted method
     * is called exactly once on all registered listeners.
     *
     * Since a task is unregistered after the message is processed, it implies that the threads responsible to produce
     * the response must have started prior to it (i.e. startThreadResourceTracking called before unregister).
     * This ensures that the number of active threads doesn't drop to zero pre-maturely.
     *
     * Rarely, some threads may even start execution after the task is unregistered. As resource stats are piggy-backed
     * with the response, any thread usage info captured after the task is unregistered may be irrelevant.
     *
     * @return the number of active resource tracking threads.
     */
    public int decrementResourceTrackingThreads() {
        int count = numActiveResourceTrackingThreads.decrementAndGet();

        if (count == 0) {
            List<Exception> listenerExceptions = new ArrayList<>();
            resourceTrackingCompletionListeners.forEach(listener -> {
                try {
                    listener.onResponse(this);
                } catch (Exception e1) {
                    try {
                        listener.onFailure(e1);
                    } catch (Exception e2) {
                        listenerExceptions.add(e2);
                    }
                }
            });
            ExceptionsHelper.maybeThrowRuntimeAndSuppress(listenerExceptions);
        }

        return count;
    }
}
