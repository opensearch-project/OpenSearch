/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;

/**
 * Client used to interact with Task Store/Queue
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TaskClient {

    /**
     * Submit a new task to Task Store/Queue
     *
     * @param task
     */
    void submitTask(Task task);

    /**
     * Claim task from Task Store/Queue. This ensure no 2 nodes work on the same task.
     *
     * @param taskId
     */
    void claimTask(TaskId taskId);

    /**
     * Get task from Task Store/Queue
     *
     * @param taskId
     * @return
     */
    Task getTask(TaskId taskId);

    /**
     * Update task in Task Store/Queue
     *
     * @param task
     * @return
     */
    Task updateTask(Task task);

    /**
     * Mark task as cancelled
     *
     * @param taskId
     */
    void cancelTask(TaskId taskId);

    /**
     * List all unassigned tasks
     *
     * @return
     */
    List<Task> getUnassignedTasks();

    /**
     * List all active tasks
     *
     * @return
     */
    List<Task> getActiveTasks();

    /**
     * List all completed tasks
     *
     * @return
     */
    List<Task> getCompletedTasks();

    /**
     * Sends task heart beat to Task Store/Queue
     *
     * @param taskId
     * @param timestamp
     */
    void sendTaskHeartbeat(TaskId taskId, long timestamp);
}
