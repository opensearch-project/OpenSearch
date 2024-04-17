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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.serializer.protobuf.SearchHitsProtobufDeserializer;
import org.opensearch.server.proto.FetchSearchResultProto;
import org.opensearch.server.proto.ShardSearchRequestProto;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Result from a fetch
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class FetchSearchResult extends SearchPhaseResult {

    private SearchHits hits;
    // client side counter
    private transient int counter;

    private FetchSearchResultProto.FetchSearchResult fetchSearchResultProto;

    public FetchSearchResult() {}

    public FetchSearchResult(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        hits = new SearchHits(in);
    }

    public FetchSearchResult(InputStream in) throws IOException {
        this.fetchSearchResultProto = FetchSearchResultProto.FetchSearchResult.parseFrom(in);
        contextId = new ShardSearchContextId(
            this.fetchSearchResultProto.getContextId().getSessionId(),
            this.fetchSearchResultProto.getContextId().getId()
        );
        SearchHitsProtobufDeserializer protobufSerializer = new SearchHitsProtobufDeserializer();
        hits = protobufSerializer.createSearchHits(new ByteArrayInputStream(this.fetchSearchResultProto.getHits().toByteArray()));
    }

    public FetchSearchResult(ShardSearchContextId id, SearchShardTarget shardTarget) {
        this.contextId = id;
        setSearchShardTarget(shardTarget);
        this.fetchSearchResultProto = FetchSearchResultProto.FetchSearchResult.newBuilder()
            .setContextId(
                ShardSearchRequestProto.ShardSearchContextId.newBuilder().setSessionId(id.getSessionId()).setId(id.getId()).build()
            )
            .build();
    }

    @Override
    public QuerySearchResult queryResult() {
        return null;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return this;
    }

    public void hits(SearchHits hits) {
        assert assertNoSearchTarget(hits);
        this.hits = hits;
        if (FeatureFlags.isEnabled(FeatureFlags.PROTOBUF_SETTING) && this.fetchSearchResultProto != null) {
            this.fetchSearchResultProto = this.fetchSearchResultProto.toBuilder()
                .setHits(SearchHitsProtobufDeserializer.convertHitsToProto(hits))
                .build();
        }
    }

    private boolean assertNoSearchTarget(SearchHits hits) {
        for (SearchHit hit : hits.getHits()) {
            assert hit.getShard() == null : "expected null but got: " + hit.getShard();
        }
        return true;
    }

    public SearchHits hits() {
        if (FeatureFlags.isEnabled(FeatureFlags.PROTOBUF_SETTING) && this.fetchSearchResultProto != null) {
            SearchHits hits;
            try {
                SearchHitsProtobufDeserializer protobufSerializer = new SearchHitsProtobufDeserializer();
                hits = protobufSerializer.createSearchHits(new ByteArrayInputStream(this.fetchSearchResultProto.getHits().toByteArray()));
                return hits;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return hits;
    }

    public FetchSearchResult initCounter() {
        counter = 0;
        return this;
    }

    public int counterGetAndIncrement() {
        return counter++;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        contextId.writeTo(out);
        hits.writeTo(out);
    }
}
