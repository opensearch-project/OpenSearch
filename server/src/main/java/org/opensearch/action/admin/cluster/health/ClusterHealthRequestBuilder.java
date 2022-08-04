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

package org.opensearch.action.admin.cluster.health;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.Priority;
import org.opensearch.common.unit.TimeValue;

/**
 * Builder for requesting cluster health
 *
 * @opensearch.internal
 */
public class ClusterHealthRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    ClusterHealthRequest,
    ClusterHealthResponse,
    ClusterHealthRequestBuilder> {

    public ClusterHealthRequestBuilder(OpenSearchClient client, ClusterHealthAction action) {
        super(client, action, new ClusterHealthRequest());
    }

    public ClusterHealthRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public ClusterHealthRequestBuilder setIndicesOptions(final IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    public ClusterHealthRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    public ClusterHealthRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForStatus(ClusterHealthStatus waitForStatus) {
        request.waitForStatus(waitForStatus);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForGreenStatus() {
        request.waitForGreenStatus();
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForYellowStatus() {
        request.waitForYellowStatus();
        return this;
    }

    /**
     * Sets whether the request should wait for there to be no relocating shards before
     * retrieving the cluster health status.  Defaults to <code>false</code>, meaning the
     * operation does not wait on there being no more relocating shards.  Set to <code>true</code>
     * to wait until the number of relocating shards in the cluster is 0.
     */
    public ClusterHealthRequestBuilder setWaitForNoRelocatingShards(boolean waitForRelocatingShards) {
        request.waitForNoRelocatingShards(waitForRelocatingShards);
        return this;
    }

    /**
     * Sets whether the request should wait for there to be no initializing shards before
     * retrieving the cluster health status.  Defaults to <code>false</code>, meaning the
     * operation does not wait on there being no more initializing shards.  Set to <code>true</code>
     * to wait until the number of initializing shards in the cluster is 0.
     */
    public ClusterHealthRequestBuilder setWaitForNoInitializingShards(boolean waitForNoInitializingShards) {
        request.waitForNoInitializingShards(waitForNoInitializingShards);
        return this;
    }

    /**
     * Sets the number of shard copies that must be active before getting the health status.
     * Defaults to {@link ActiveShardCount#NONE}, meaning we don't wait on any active shards.
     * Set this value to {@link ActiveShardCount#ALL} to wait for all shards (primary and
     * all replicas) to be active across all indices in the cluster. Otherwise, use
     * {@link ActiveShardCount#from(int)} to set this value to any non-negative integer, up to the
     * total number of shard copies that would exist across all indices in the cluster.
     */
    public ClusterHealthRequestBuilder setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        if (waitForActiveShards.equals(ActiveShardCount.DEFAULT)) {
            // the default for cluster health is 0, not 1
            request.waitForActiveShards(ActiveShardCount.NONE);
        } else {
            request.waitForActiveShards(waitForActiveShards);
        }
        return this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public ClusterHealthRequestBuilder setWaitForActiveShards(int waitForActiveShards) {
        request.waitForActiveShards(waitForActiveShards);
        return this;
    }

    /**
     * Waits for N number of nodes. Use "12" for exact mapping, "&gt;12" and "&lt;12" for range.
     */
    public ClusterHealthRequestBuilder setWaitForNodes(String waitForNodes) {
        request.waitForNodes(waitForNodes);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForEvents(Priority waitForEvents) {
        request.waitForEvents(waitForEvents);
        return this;
    }
}
