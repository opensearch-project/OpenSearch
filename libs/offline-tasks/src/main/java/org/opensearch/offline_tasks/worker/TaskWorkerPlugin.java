/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks.worker;

import org.opensearch.offline_tasks.task.TaskType;

import java.util.Map;

/**
 * Plugin for providing offline nodes worker for a TaskType
 */
public interface TaskWorkerPlugin {

    /**
     * Get the new TaskWorker for a TaskType
     *
     * @return TaskWorker to execute Tasks on Offline Nodes
     */
    Map<TaskType, TaskWorker> getTaskWorker();
}
