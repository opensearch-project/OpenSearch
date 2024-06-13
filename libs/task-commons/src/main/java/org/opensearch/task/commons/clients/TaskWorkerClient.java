/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.task.commons.clients;

import org.opensearch.task.commons.task.Task;
import org.opensearch.task.commons.task.TaskId;

import java.util.List;

/**
 * Consumer interface used to find new tasks assigned to a {@code WorkerNode} for execution.
 */
public interface TaskWorkerClient {

    /**
     * List all tasks assigned to a WorkerNode.
     * Useful when the implementation uses a separate store for Task assignments to Worker nodes
     *
     * @param taskListRequest TaskListRequest
     * @return list of all tasks assigned to a WorkerNode
     */
    List<Task> getAssignedTasks(TaskListRequest taskListRequest);

    /**
     * Sends task heart beat to Task Store/Queue
     *
     * @param taskId TaskId of Task to send heartbeat for
     * @param timestamp timestamp of heartbeat to be recorded in TaskStore/Queue
     */
    void sendTaskHeartbeat(TaskId taskId, long timestamp);

}
