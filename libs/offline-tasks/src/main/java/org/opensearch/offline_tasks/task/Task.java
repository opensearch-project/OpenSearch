/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks.task;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * A Background Task to be run on Offline Node.
 */
@ExperimentalApi
public class Task {

    /**
     * Task identifier used to uniquely identify a Task
     */
    private final TaskId taskId;

    /**
     * Various params to used for Task execution
     */
    private final TaskParams params;

    /**
     * Type/Category of the Task
     */
    private final TaskType taskType;

    /**
     * Constructor for Task
     *
     * @param taskId Task identifier
     * @param params Task Params
     * @param taskType Task Type
     */
    public Task(TaskId taskId, TaskParams params, TaskType taskType) {
        this.taskId = taskId;
        this.params = params;
        this.taskType = taskType;
    }

    /**
     * Get TaskId
     * @return TaskId
     */
    public TaskId getTaskId() {
        return taskId;
    }

    /**
     * Get TaskParams
     * @return TaskParams
     */
    public TaskParams getParams() {
        return params;
    }

    /**
     * Get TaskType
     * @return TaskType
     */
    public TaskType getTaskType() {
        return taskType;
    }
}
