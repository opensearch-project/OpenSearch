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
import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.NamedWriteable;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Current task information
 */
public class Task {

    private static final Logger logger = LogManager.getLogger(Task.class);

    /**
     * The request header to mark tasks with specific ids
     */
    public static final String X_OPAQUE_ID = "X-Opaque-Id";

    private static final String TOTAL = "total";

    private final long id;

    private final String type;

    private final String action;

    private final String description;

    private final TaskId parentTask;

    private final Map<String, String> headers;

    private final Map<Long, List<ThreadResourceInfo>> resourceStats;

    /**
     * The task's start time as a wall clock time since epoch ({@link System#currentTimeMillis()} style).
     */
    private final long startTime;

    /**
     * The task's start time as a relative time ({@link System#nanoTime()} style).
     */
    private final long startTimeNanos;

    public Task(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        this(id, type, action, description, parentTask, System.currentTimeMillis(), System.nanoTime(), headers, new ConcurrentHashMap<>());
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
        ConcurrentHashMap<Long, List<ThreadResourceInfo>> resourceStats
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
        return taskInfo(localNodeId, detailed, !detailed);
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
            resourceStats = new TaskResourceStats(new HashMap<String, TaskResourceUsage>() {
                {
                    put(TOTAL, getTotalResourceStats());
                }
            });
        }
        return taskInfo(localNodeId, description, status, resourceStats);
    }

    /**
     * Build a proper {@link TaskInfo} for this task.
     */
    protected final TaskInfo taskInfo(String localNodeId, String description, Status status, TaskResourceStats resourceStats) {
        return new TaskInfo(
            new TaskId(localNodeId, getId()),
            getType(),
            getAction(),
            description,
            status,
            startTime,
            System.nanoTime() - startTimeNanos,
            this instanceof CancellableTask,
            this instanceof CancellableTask && ((CancellableTask) this).isCancelled(),
            parentTask,
            headers,
            resourceStats
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
        return resourceStats;
    }

    /**
     * Returns total resource usage of the task
     */
    public TaskResourceUsage getTotalResourceStats() {
        return new TaskResourceUsage(getTotalResourceUtilization(ResourceStats.CPU), getTotalResourceUtilization(ResourceStats.MEMORY));
    }

    /**
     * Returns total resource consumption for a specific task stat.
     */
    public long getTotalResourceUtilization(ResourceStats taskStats) {
        long totalResourceConsumption = 0L;
        for (List<ThreadResourceInfo> threadResourceInfosList : resourceStats.values()) {
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfosList) {
                for (Map.Entry<ResourceStatsType, ResourceUsageInfo> entry : threadResourceInfo.getResourceUsageInfos().entrySet()) {
                    if (entry.getKey().isOnlyForAnalysis() == false) {
                        totalResourceConsumption += entry.getValue().getStatsInfo().get(taskStats).getTotalValue();
                    }
                }
            }
        }
        return totalResourceConsumption;
    }

    /**
     * Adds thread's resource consumption information
     * @param threadId ID of the thread
     * @param statsType stats type
     * @param resourceUsageMetrics resource consumption metrics of the thread
     */
    public void startThreadResourceTracking(long threadId, ResourceStatsType statsType, ResourceUsageMetric... resourceUsageMetrics) {
        if (statsType != ResourceStatsType.WORKER_STATS) {
            throw new IllegalArgumentException("Adding thread resource information should always have WORKER_STATS as stats type");
        }
        final List<ThreadResourceInfo> threadResourceInfoList = resourceStats.computeIfAbsent(threadId, k -> new ArrayList<>());
        // active thread entry should not be present in the list.
        for (ThreadResourceInfo threadResourceInfo : threadResourceInfoList) {
            if (threadResourceInfo.isActive()) {
                throw new IllegalStateException("Unexpected active thread entry is present");
            }
        }
        threadResourceInfoList.add(new ThreadResourceInfo(ResourceStatsType.WORKER_STATS, resourceUsageMetrics));
    }

    /**
     * This method is used to update the resource consumption stats so that the data isn't too stale for long-running task.
     * If an active thread entry is not present in the list, the update is dropped.
     * @param threadId ID of the thread
     * @param statsType stats type
     * @param resourceUsageMetrics resource consumption metrics of the thread
     */
    public void updateThreadResourceStats(long threadId, ResourceStatsType statsType, ResourceUsageMetric... resourceUsageMetrics) {
        final List<ThreadResourceInfo> threadResourceInfoList = resourceStats.get(threadId);
        if (threadResourceInfoList == null) {
            throw new IllegalStateException("Cannot update if thread resource info is not present");
        } else {
            // If active entry is not present, the update is dropped. If present, the active entry is updated.
            for (ThreadResourceInfo threadResourceInfo : threadResourceInfoList) {
                if (threadResourceInfo.isActive()) {
                    threadResourceInfo.updateResourceInfo(statsType, resourceUsageMetrics);
                    return;
                }
            }
        }
    }

    /**
     * Record the thread's final resource consumption values.
     * @param threadId ID of the thread
     * @param statsType stats type
     * @param resourceUsageMetrics resource consumption metrics of the thread
     */
    public void stopThreadResourceTracking(long threadId, ResourceStatsType statsType, ResourceUsageMetric... resourceUsageMetrics) {
        final List<ThreadResourceInfo> threadResourceInfoList = resourceStats.get(threadId);
        if (statsType != ResourceStatsType.WORKER_STATS || threadResourceInfoList == null) {
            throw new IllegalArgumentException(
                "Recording the end should have WORKER_STATS as stats type" + "and an active entry should be present in the list"
            );
        }
        // marking active entries as done before updating the final resource usage values.
        for (ThreadResourceInfo threadResourceInfo : threadResourceInfoList) {
            if (threadResourceInfo.isActive()) {
                threadResourceInfo.setActive(false);
                threadResourceInfo.updateResourceInfo(ResourceStatsType.WORKER_STATS, resourceUsageMetrics);
            }
        }
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
}
