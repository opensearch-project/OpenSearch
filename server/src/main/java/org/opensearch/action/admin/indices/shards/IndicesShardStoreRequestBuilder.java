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

package org.opensearch.action.admin.indices.shards;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.MasterNodeReadOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.health.ClusterHealthStatus;

/**
 * Request builder for {@link IndicesShardStoresRequest}
 *
 * @opensearch.internal
 */
public class IndicesShardStoreRequestBuilder extends MasterNodeReadOperationRequestBuilder<
    IndicesShardStoresRequest,
    IndicesShardStoresResponse,
    IndicesShardStoreRequestBuilder> {

    public IndicesShardStoreRequestBuilder(OpenSearchClient client, ActionType<IndicesShardStoresResponse> action, String... indices) {
        super(client, action, new IndicesShardStoresRequest(indices));
    }

    /**
     * Sets the indices for the shard stores request
     */
    public IndicesShardStoreRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * By default, expands wildcards to both open and closed indices
     */
    public IndicesShardStoreRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Set statuses to filter shards to get stores info on.
     * @param shardStatuses acceptable values are "green", "yellow", "red" and "all"
     * see {@link ClusterHealthStatus} for details
     */
    public IndicesShardStoreRequestBuilder setShardStatuses(String... shardStatuses) {
        request.shardStatuses(shardStatuses);
        return this;
    }
}
