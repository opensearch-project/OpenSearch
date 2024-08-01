/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.task.commons.task;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.task.commons.worker.WorkerNode;

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
     * Worker Node on which the Task is to be executed
     */
    private final WorkerNode assignedNode;

    /**
     * Timestamp at which the Task was created
     */
    private final long createdAt;

    /**
     * Timestamp at which the Task was assigned to a worker
     */
    private final long assignedAt;

    /**
     * Timestamp at which the Task was started execution on worker
     */
    private final long startedAt;

    /**
     * Timestamp at which the Task was either completed/failed/cancelled
     */
    private final long completedAt;

    /**
     * Timestamp at which last heartbeat was sent by the worker
     */
    private final long lastHeartbeatAt;

    /**
     * Constructor for Task
     *
     * @param taskId Task identifier
     * @param taskStatus Task status
     * @param params Task Params
     * @param taskType Task Type
     * @param createdAt Timestamp at which the Task was created
     * @param assignedAt Timestamp at which the Task was assigned to a worker
     * @param startedAt Timestamp at which the Task was started execution on worker
     * @param completedAt Timestamp at which the Task was either completed/failed/cancelled
     * @param lastHeartbeatAt Timestamp at which last heartbeat was sent by the worker
     * @param assignedNode Worker Node on which the Task is to be executed
     */
    public Task(
        TaskId taskId,
        TaskStatus taskStatus,
        TaskParams params,
        TaskType taskType,
        long createdAt,
        @Nullable long assignedAt,
        @Nullable long startedAt,
        @Nullable long completedAt,
        @Nullable long lastHeartbeatAt,
        @Nullable WorkerNode assignedNode
    ) {
        this.taskId = taskId;
        this.taskStatus = taskStatus;
        this.params = params;
        this.taskType = taskType;
        this.createdAt = createdAt;
        this.assignedAt = assignedAt;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.lastHeartbeatAt = lastHeartbeatAt;
        this.assignedNode = assignedNode;
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
    public long getAssignedAt() {
        return assignedAt;
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
     * Get Last Heartbeat Time
     * @return lastHeartbeatAt
     */
    public long getLastHeartbeatAt() {
        return lastHeartbeatAt;
    }

    /**
     * Get Task Assigned Node
     * @return assignedNode
     */
    public WorkerNode getAssignedNode() {
        return assignedNode;
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
         * Type/Category of the Task
         */
        private WorkerNode assignedNode;

        /**
         * Timestamp at which the Task was created
         */
        private final long createdAt;

        /**
         * Timestamp at which the Task was assigned to a worker
         */
        private long assignedAt;

        /**
         * Timestamp at which the Task was started execution on worker
         */
        private long startedAt;

        /**
         * Timestamp at which the Task was either completed/failed/cancelled
         */
        private long completedAt;

        /**
         * Timestamp at which last heartbeat was sent by the worker
         */
        private long lastHeartbeatAt;

        /**
         * Constructor for Task Builder
         *
         * @param taskId Task identifier
         * @param taskStatus Task status
         * @param params Task Params
         * @param taskType Task Type
         * @param createdAt Task Creation Time
         */
        private Builder(TaskId taskId, TaskStatus taskStatus, TaskParams params, TaskType taskType, long createdAt) {
            this.taskId = taskId;
            this.taskStatus = taskStatus;
            this.params = params;
            this.taskType = taskType;
            this.createdAt = createdAt;
        }

        /**
         * Build Builder from Task
         * @param task Task to build from
         * @return Task.Builder
         */
        public static Builder builder(Task task) {
            Builder builder = new Builder(
                task.getTaskId(),
                task.getTaskStatus(),
                task.getParams(),
                task.getTaskType(),
                task.getCreatedAt()
            );
            builder.assignedAt(task.getAssignedAt());
            builder.startedAt(task.getStartedAt());
            builder.completedAt(task.getCompletedAt());
            builder.lastHeartbeatAt(task.getLastHeartbeatAt());
            builder.assignedNode(task.getAssignedNode());
            return builder;
        }

        /**
         * Build Builder from various Task attributes
         * @param taskId Task identifier
         * @param taskStatus TaskStatus
         * @param params TaskParams
         * @param taskType TaskType
         * @param createdAt Task Creation Time
         * @return Task.Builder
         */
        public static Builder builder(TaskId taskId, TaskStatus taskStatus, TaskParams params, TaskType taskType, long createdAt) {
            return new Builder(taskId, taskStatus, params, taskType, createdAt);
        }

        /**
         * Set Task Assignment Time
         * @param assignedAt Timestamp at which the Task was assigned to a worker
         */
        public void assignedAt(long assignedAt) {
            this.assignedAt = assignedAt;
        }

        /**
         * Set Task Start Time
         * @param startedAt Timestamp at which the Task was started execution on worker
         */
        public void startedAt(long startedAt) {
            this.startedAt = startedAt;
        }

        /**
         * Set Task Completion Time
         * @param completedAt Timestamp at which the Task was either completed/failed/cancelled
         */
        public void completedAt(long completedAt) {
            this.completedAt = completedAt;
        }

        /**
         * Set Task Last Heartbeat Time for the task
         * @param lastHeartbeatAt Timestamp at which last heartbeat was sent by the worker
         */
        public void lastHeartbeatAt(long lastHeartbeatAt) {
            this.lastHeartbeatAt = lastHeartbeatAt;
        }

        /**
         * Set Task Assigned Node
         * @param node Worker Node on which the Task is to be executed
         */
        public void assignedNode(WorkerNode node) {
            this.assignedNode = node;
        }

        /**
         * Build Task from Builder
         * @return Task
         */
        public Task build() {
            return new Task(
                taskId,
                taskStatus,
                params,
                taskType,
                createdAt,
                assignedAt,
                startedAt,
                completedAt,
                lastHeartbeatAt,
                assignedNode
            );
        }
    }
}
