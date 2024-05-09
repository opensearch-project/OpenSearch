/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.offline_tasks.task.Task;
import org.opensearch.offline_tasks.task.TaskId;

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
     */
    void claimTask(TaskId taskId);

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
     * List all unassigned tasks
     *
     * @return list of all the task which are note not assigned to any worker
     */
    List<Task> getUnassignedTasks();

    /**
     * List all active tasks
     *
     * @return list of all the task which are running on any worker
     */
    List<Task> getActiveTasks();

    /**
     * List all completed tasks
     *
     * @return list of all the task which have completed execution
     */
    List<Task> getCompletedTasks();

    /**
     * Sends task heart beat to Task Store/Queue
     *
     * @param taskId TaskId of Task to send heartbeat for
     * @param timestamp timestamp of heartbeat to be recorded in TaskStore/Queue
     */
    void sendTaskHeartbeat(TaskId taskId, long timestamp);
}
