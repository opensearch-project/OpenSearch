/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.task.commons.task.TaskType;
import org.opensearch.task.commons.worker.TaskWorker;
import org.opensearch.threadpool.ThreadPool;

/**
 * Plugin for providing TaskWorkers for Offline Nodes
 */
@ExperimentalApi
public interface TaskWorkerPlugin {

    /**
     * Get the new TaskWorker for a TaskType
     *
     * @return TaskWorker to execute Tasks on Offline Nodes
     */
    TaskWorker getTaskWorker(Client client, ClusterService clusterService, ThreadPool threadPool);

    /**
     * Get the TaskType for this TaskWorker
     * @return TaskType
     */
    TaskType getTaskType();
}
