/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.task.commons.clients;

import org.opensearch.task.commons.task.TaskStatus;
import org.opensearch.task.commons.task.TaskType;
import org.opensearch.task.commons.worker.WorkerNode;

/**
 * Request object for listing tasks
 */
public class TaskListRequest {

    /**
     * Filters listTasks response by specific task status'
     */
    private TaskStatus[] taskStatus;

    /**
     * Filter listTasks response by specific task types
     */
    private TaskType[] taskTypes;

    /**
     * Filter listTasks response by specific worker node
     */
    private WorkerNode workerNodes;

    /**
     * Depicts the start page number for the list call.
     *
     * @see TaskManagerClient#listTasks(TaskListRequest)
     */
    private int startPageNumber;

    /**
     * Depicts the page size for the list call.
     *
     * @see TaskManagerClient#listTasks(TaskListRequest)
     */
    private int pageSize;

    /**
     * Default constructor
     */
    public TaskListRequest() {}

    /**
     * Update task types to filter with in the request
     * @param taskTypes TaskType[]
     * @return ListTaskRequest
     */
    public TaskListRequest taskType(TaskType... taskTypes) {
        this.taskTypes = taskTypes;
        return this;
    }

    /**
     * Update task status to filter with in the request
     * @param taskStatus TaskStatus[]
     * @return ListTaskRequest
     */
    public TaskListRequest taskType(TaskStatus... taskStatus) {
        this.taskStatus = taskStatus;
        return this;
    }

    /**
     * Update worker node to filter with in the request
     * @param workerNode WorkerNode
     * @return ListTaskRequest
     */
    private TaskListRequest workerNode(WorkerNode workerNode) {
        this.workerNodes = workerNode;
        return this;
    }

    /**
     * Update page number to start with when fetching the list of tasks
     * @param startPageNumber startPageNumber
     * @return ListTaskRequest
     */
    public TaskListRequest startPageNumber(int startPageNumber) {
        this.startPageNumber = startPageNumber;
        return this;
    }

    /**
     * Update page size for the list tasks response
     * @param pageSize int
     * @return ListTaskRequest
     */
    public TaskListRequest pageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }
}
