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

package org.opensearch.search.fetch;

import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.server.proto.QueryFetchSearchResultProto;
import org.opensearch.transport.nativeprotocol.NativeInboundMessage;
import org.opensearch.transport.protobufprotocol.ProtobufInboundMessage;

import java.io.IOException;
import java.io.InputStream;

/**
 * Query fetch result
 *
 * @opensearch.internal
 */
public final class QueryFetchSearchResult extends SearchPhaseResult {

    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;

    private QueryFetchSearchResultProto.QueryFetchSearchResult queryFetchSearchResultProto;

    public QueryFetchSearchResult(StreamInput in) throws IOException {
        super(in);
        queryResult = new QuerySearchResult(in);
        fetchResult = new FetchSearchResult(in);
    }

    public QueryFetchSearchResult(InputStream in) throws IOException {
        super(in);
        this.queryFetchSearchResultProto = QueryFetchSearchResultProto.QueryFetchSearchResult.parseFrom(in);
        queryResult = new QuerySearchResult(in);
        fetchResult = new FetchSearchResult(in);
    }

    public QueryFetchSearchResult(QuerySearchResult queryResult, FetchSearchResult fetchResult) {
        this.queryResult = queryResult;
        this.fetchResult = fetchResult;
        if (queryResult.response() != null && fetchResult.response() != null) {
            this.queryFetchSearchResultProto = QueryFetchSearchResultProto.QueryFetchSearchResult.newBuilder()
                .setQueryResult(queryResult.response())
                .setFetchResult(fetchResult.response())
                .build();
        }
    }

    @Override
    public ShardSearchContextId getContextId() {
        return queryResult.getContextId();
    }

    @Override
    public SearchShardTarget getSearchShardTarget() {
        return queryResult.getSearchShardTarget();
    }

    @Override
    public void setSearchShardTarget(SearchShardTarget shardTarget) {
        super.setSearchShardTarget(shardTarget);
        queryResult.setSearchShardTarget(shardTarget);
        fetchResult.setSearchShardTarget(shardTarget);
    }

    @Override
    public void setShardIndex(int requestIndex) {
        super.setShardIndex(requestIndex);
        queryResult.setShardIndex(requestIndex);
        fetchResult.setShardIndex(requestIndex);
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        queryResult.writeTo(out);
        fetchResult.writeTo(out);
    }

    @Override
    public String getProtocol() {
        if (FeatureFlags.isEnabled(FeatureFlags.PROTOBUF_SETTING)) {
            return ProtobufInboundMessage.PROTOBUF_PROTOCOL;
        }
        return NativeInboundMessage.NATIVE_PROTOCOL;
    }

    public QueryFetchSearchResultProto.QueryFetchSearchResult response() {
        return this.queryFetchSearchResultProto;
    }

    public QueryFetchSearchResult(QueryFetchSearchResultProto.QueryFetchSearchResult queryFetchSearchResult) {
        this.queryFetchSearchResultProto = queryFetchSearchResult;
        this.queryResult = new QuerySearchResult(queryFetchSearchResult.getQueryResult());
        this.fetchResult = new FetchSearchResult(queryFetchSearchResult.getFetchResult());
    }

}
