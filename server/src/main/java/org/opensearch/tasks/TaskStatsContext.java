/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import java.util.Map;

public class TaskStatsContext {
    private final long taskId;
    private final String type;
    private final String action;
    private final String description;
    private final long startTimeNanos;
    private final String parentTaskId;
    private final Map<String, Long> allStats;

    private TaskStatsContext(
        long taskId,
        String type,
        String action,
        String description,
        long startTimeNanos,
        String parentTaskId,
        Map<String, Long> allStats
    ) {
        this.taskId = taskId;
        this.type = type;
        this.action = action;
        this.description = description;
        this.startTimeNanos = startTimeNanos;
        this.parentTaskId = parentTaskId;
        this.allStats = allStats;
    }

    public long getTaskId() {
        return taskId;
    }

    public String getType() {
        return type;
    }

    public String getAction() {
        return action;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Returns the task start time
     */
    public long getStartTime() {
        return startTimeNanos;
    }

    /**
     * Returns the task running time
     */
    public long getRunningTimeNanos() {
        return System.nanoTime() - startTimeNanos;
    }

    /**
     * Returns the parent task id
     */
    public String getParentTaskId() {
        return parentTaskId;
    }

    public Map<String, Long> getAllStats() {
        return allStats;
    }

    public static TaskStatsContext createTaskStatsContext(Task task) {
        return new TaskStatsContext(
            task.getId(),
            task.getType(),
            task.getAction(),
            task.getDescription(),
            task.getStartTimeNanos(),
            task.getParentTaskId().toString(),
            task.getTotalResourceStats()
        );

    }
}
