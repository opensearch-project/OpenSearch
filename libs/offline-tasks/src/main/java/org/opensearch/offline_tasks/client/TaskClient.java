/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks.client;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.offline_tasks.task.Task;
import org.opensearch.offline_tasks.task.TaskId;
import org.opensearch.offline_tasks.task.TaskStatus;

import java.util.List;

/**
 * Client used to interact with Task Store/Queue
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TaskClient {

    /**
     * Submit a new task to TaskStore/Queue
     *
     * @param task Task to be submitted for execution on offline nodes
     */
    void submitTask(Task task);

    /**
     * Claim task from TaskStore/Queue. This ensures no 2 Offline Nodes work on the same task.
     *
     * @param taskId TaskId of the task to be claimed
     * @return true if task is claimed successfully, false otherwise
     */
    boolean claimTask(TaskId taskId);

    /**
     * Get task from TaskStore/Queue
     *
     * @param taskId TaskId of the task to be retrieved
     * @return Task corresponding to TaskId
     */
    Task getTask(TaskId taskId);

    /**
     * Update task in TaskStore/Queue
     *
     * @param task Task to be updated
     */
    void updateTask(Task task);

    /**
     * Mark task as cancelled.
     * Ongoing Tasks can be cancelled as well if the corresponding worker supports cancellation
     *
     * @param taskId TaskId of the task to be cancelled
     */
    void cancelTask(TaskId taskId);

    /**
     * List all tasks with a particular {@param taskStatus} considering {@param listTaskStatus}
     * @param taskStatus status of the tasks to be listed
     * @param taskListQueryParams params to filter the tasks to be listed
     * @return list of all the task matching the taskStatus
     */
    List<Task> getTasks(TaskStatus taskStatus, TaskListQueryParams taskListQueryParams);

    /**
     * List all tasks considering {@param listTaskStatus}
     * @param taskListQueryParams params to filter the tasks to be listed
     * @return list of all the task matching the taskStatus
     */
    List<Task> getTasks(TaskListQueryParams taskListQueryParams);

    /**
     * Sends task heart beat to Task Store/Queue
     *
     * @param taskId TaskId of Task to send heartbeat for
     * @param timestamp timestamp of heartbeat to be recorded in TaskStore/Queue
     */
    void sendTaskHeartbeat(TaskId taskId, long timestamp);
}
