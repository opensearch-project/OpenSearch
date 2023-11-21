/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster;

import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.common.annotation.PublicApi;

import java.util.List;

/**
 * Interface to implement a cluster state change listener
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface ClusterStateTaskListener {

    /**
     * A callback called when execute fails.
     */
    void onFailure(String source, Exception e);

    /**
     * called when the task was rejected because the local node is no longer cluster-manager.
     * Used only for tasks submitted to {@link ClusterManagerService}.
     */
    default void onNoLongerClusterManager(String source) {
        onFailure(source, new NotClusterManagerException("no longer cluster-manager. source: [" + source + "]"));
    }

    /**
     * called when the task was rejected because the local node is no longer cluster-manager.
     * Used only for tasks submitted to {@link ClusterManagerService}.
     *
     * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #onNoLongerClusterManager(String)}
     */
    @Deprecated
    default void onNoLongerMaster(String source) {
        onNoLongerClusterManager(source);
    }

    /**
     * Called when the result of the {@link ClusterStateTaskExecutor#execute(ClusterState, List)} have been processed
     * properly by all listeners.
     */
    default void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {}
}
