/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

/**
 * Main Cluster Manager Node Service
 *
 * @opensearch.api
 */
@PublicApi(since = "2.2.0")
public class ClusterManagerService extends MasterService {
    public ClusterManagerService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        super(settings, clusterSettings, threadPool);
    }
}
