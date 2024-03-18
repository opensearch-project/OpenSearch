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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.search;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

/**
 * This class is a base class for all search related results. It contains the shard target it
 * was executed against, a shard index used to reference the result on the coordinating node
 * and a request ID that is used to reference the request context on the executing node. The
 * request ID is particularly important since it is used to reference and maintain a context
 * across search phases to ensure the same point in time snapshot is used for querying and
 * fetching etc.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class SearchPhaseResult extends TransportResponse {

    private SearchShardTarget searchShardTarget;
    private int shardIndex = -1;
    protected ShardSearchContextId contextId;
    private ShardSearchRequest shardSearchRequest;
    private RescoreDocIds rescoreDocIds = RescoreDocIds.EMPTY;

    protected SearchPhaseResult() {

    }

    protected SearchPhaseResult(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Returns the search context ID that is used to reference the search context on the executing node
     * or <code>null</code> if no context was created.
     */
    @Nullable
    public ShardSearchContextId getContextId() {
        return contextId;
    }

    /**
     * Returns the shard index in the context of the currently executing search request that is
     * used for accounting on the coordinating node
     */
    public int getShardIndex() {
        assert shardIndex != -1 : "shardIndex is not set";
        return shardIndex;
    }

    public SearchShardTarget getSearchShardTarget() {
        return searchShardTarget;
    }

    public void setSearchShardTarget(SearchShardTarget shardTarget) {
        this.searchShardTarget = shardTarget;
    }

    public void setShardIndex(int shardIndex) {
        assert shardIndex >= 0 : "shardIndex must be >= 0 but was: " + shardIndex;
        this.shardIndex = shardIndex;
    }

    /**
     * Returns the query result iff it's included in this response otherwise <code>null</code>
     */
    public QuerySearchResult queryResult() {
        return null;
    }

    /**
     * Returns the fetch result iff it's included in this response otherwise <code>null</code>
     */
    public FetchSearchResult fetchResult() {
        return null;
    }

    @Nullable
    public ShardSearchRequest getShardSearchRequest() {
        return shardSearchRequest;
    }

    public void setShardSearchRequest(ShardSearchRequest shardSearchRequest) {
        this.shardSearchRequest = shardSearchRequest;
    }

    public RescoreDocIds getRescoreDocIds() {
        return rescoreDocIds;
    }

    public void setRescoreDocIds(RescoreDocIds rescoreDocIds) {
        this.rescoreDocIds = rescoreDocIds;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: this seems wrong, SearchPhaseResult should have a writeTo?
    }
}
