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

import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;

import java.util.List;

/**
 * A task that can update the cluster state.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class ClusterStateUpdateTask
    implements
        ClusterStateTaskConfig,
        ClusterStateTaskExecutor<ClusterStateUpdateTask>,
        ClusterStateTaskListener {

    private final Priority priority;

    public ClusterStateUpdateTask() {
        this(Priority.NORMAL);
    }

    public ClusterStateUpdateTask(Priority priority) {
        this.priority = priority;
    }

    @Override
    public final ClusterTasksResult<ClusterStateUpdateTask> execute(ClusterState currentState, List<ClusterStateUpdateTask> tasks)
        throws Exception {
        ClusterState result = execute(currentState);
        ClusterStatePublisher.ClusterStateUpdateResult updateResult = null;
        if(result == null) {
            updateResult = executeAndReturnChangeResult(currentState);
            result = updateResult.getUpdatedState(currentState);
        }
        return ClusterTasksResult.<ClusterStateUpdateTask>builder().successes(tasks).build(result,updateResult);
    }

    @Override
    public String describeTasks(List<ClusterStateUpdateTask> tasks) {
        return ""; // one of task, source is enough
    }

    /**
     * Update the cluster state based on the current state. Return the *same instance* if no state
     * should be changed.
     */
    public abstract ClusterState execute(ClusterState currentState) throws Exception;

    public ClusterStatePublisher.ClusterStateUpdateResult executeAndReturnChangeResult(ClusterState state) throws Exception {
        return null;
    }
    /**
     * A callback called when execute fails.
     */
    public abstract void onFailure(String source, Exception e);

    @Override
    public final void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
        // final, empty implementation here as this method should only be defined in combination
        // with a batching executor as it will always be executed within the system context.
    }

    /**
     * If the cluster state update task wasn't processed by the provided timeout, call
     * {@link ClusterStateTaskListener#onFailure(String, Exception)}. May return null to indicate no timeout is needed (default).
     */
    @Nullable
    public TimeValue timeout() {
        return null;
    }

    @Override
    public Priority priority() {
        return priority;
    }

    /**
     * Marked as final as cluster state update tasks should only run on cluster-manager.
     * For local requests, use {@link LocalClusterUpdateTask} instead.
     */
    @Override
    public final boolean runOnlyOnClusterManager() {
        return true;
    }
}
