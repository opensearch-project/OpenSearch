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
     * Depicts latest state of the Task
     */
    private final TaskStatus taskStatus;

    /**
     * Various params to used for Task execution
     */
    private final TaskParams params;

    /**
     * Type/Category of the Task
     */
    private final TaskType taskType;

    /**
     * Timestamp at which the Task was created
     */
    private final long createdAt;

    /**
     * Timestamp at which the Task was assigned to a worker
     */
    private final long claimedAt;

    /**
     * Timestamp at which the Task was started execution on worker
     */
    private final long startedAt;

    /**
     * Timestamp at which the Task was either completed/failed/cancelled
     */
    private final long completedAt;

    /**
     * Constructor for Task
     *
     * @param taskId Task identifier
     * @param taskStatus Task status
     * @param params Task Params
     * @param taskType Task Type
     */
    // missing params to below constructor
    public Task(
        TaskId taskId,
        TaskStatus taskStatus,
        TaskParams params,
        TaskType taskType,
        long createdAt,
        long claimedAt,
        long startedAt,
        long completedAt
    ) {
        this.taskId = taskId;
        this.taskStatus = taskStatus;
        this.params = params;
        this.taskType = taskType;
        this.createdAt = createdAt;
        this.claimedAt = claimedAt;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
    }

    /**
     * Get TaskId
     * @return TaskId
     */
    public TaskId getTaskId() {
        return taskId;
    }

    /**
     * Get TaskStatus
     * @return TaskStatus
     */
    public TaskStatus getTaskStatus() {
        return taskStatus;
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

    /**
     * Get Task Creation Time
     * @return createdAt
     */
    public long getCreatedAt() {
        return createdAt;
    }

    /**
     * Get Task Assignment Time
     * @return assignedAt
     */
    public long getClaimedAt() {
        return claimedAt;
    }

    /**
     * Get Task Start Time
     * @return startedAt
     */
    public long getStartedAt() {
        return startedAt;
    }

    /**
     * Get Task Completion Time
     * @return completedAt
     */
    public long getCompletedAt() {
        return completedAt;
    }

    /**
     * Builder class for Task.
     */
    public static class Builder {
        /**
         * Task identifier used to uniquely identify a Task
         */
        private final TaskId taskId;

        /**
         * Depicts latest state of the Task
         */
        private final TaskStatus taskStatus;

        /**
         * Various params to used for Task execution
         */
        private final TaskParams params;

        /**
         * Type/Category of the Task
         */
        private final TaskType taskType;

        /**
         * Timestamp at which the Task was created
         */
        private long createdAt;

        /**
         * Timestamp at which the Task was assigned to a worker
         */
        private long claimedAt;

        /**
         * Timestamp at which the Task was started execution on worker
         */
        private long startedAt;

        /**
         * Timestamp at which the Task was either completed/failed/cancelled
         */
        private long completedAt;

        private Builder(TaskId taskId, TaskStatus taskStatus, TaskParams params, TaskType taskType) {
            this.taskId = taskId;
            this.taskStatus = taskStatus;
            this.params = params;
            this.taskType = taskType;
        }

        public Builder builder(Task task) {
            Builder builder = new Builder(task.getTaskId(), task.getTaskStatus(), task.getParams(), task.getTaskType());
            builder.createdAt(task.getCreatedAt());
            builder.claimedAt(task.getClaimedAt());
            builder.startedAt(task.getStartedAt());
            builder.completedAt(task.getCompletedAt());
            return builder;
        }

        public Builder builder(TaskId taskId, TaskStatus taskStatus, TaskParams params, TaskType taskType) {
            return new Builder(taskId, taskStatus, params, taskType);
        }

        public void createdAt(long createdAt) {
            this.createdAt = createdAt;
        }

        public void claimedAt(long claimedAt) {
            this.claimedAt = claimedAt;
        }

        public void startedAt(long startedAt) {
            this.startedAt = startedAt;
        }

        public void completedAt(long completedAt) {
            this.completedAt = completedAt;
        }

        public Task build() {
            return new Task(taskId, taskStatus, params, taskType, createdAt, claimedAt, startedAt, completedAt);
        }
    }
}
