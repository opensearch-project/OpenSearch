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

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

/**
 * Result from a fetch
 *
 * @opensearch.internal
 */
public final class FetchSearchResult extends SearchPhaseResult {

    private SearchHits hits;
    // client side counter
    private transient int counter;

    public FetchSearchResult() {}

    public FetchSearchResult(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        hits = new SearchHits(in);
    }

    public FetchSearchResult(ShardSearchContextId id, SearchShardTarget shardTarget) {
        this.contextId = id;
        setSearchShardTarget(shardTarget);
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
    }

    private boolean assertNoSearchTarget(SearchHits hits) {
        for (SearchHit hit : hits.getHits()) {
            assert hit.getShard() == null : "expected null but got: " + hit.getShard();
        }
        return true;
    }

    public SearchHits hits() {
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
