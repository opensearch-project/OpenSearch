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
import org.opensearch.offline_tasks.TaskClient;
import org.opensearch.threadpool.ThreadPool;

/**
 * Plugin to provide an implementation of Task client
 */
@ExperimentalApi
public interface OfflineTaskClientPlugin {

    /**
     * Get the task client.
     */
    TaskClient getTaskClient(Client client, ClusterService clusterService, ThreadPool threadPool);
}
