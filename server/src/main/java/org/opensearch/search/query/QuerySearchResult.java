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

package org.opensearch.search.query;

import org.apache.lucene.search.TotalHits;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.io.stream.DelayableWriteable;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.RescoreDocIds;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.profile.NetworkTime;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.query.serializer.NativeQuerySearchResultSerializer;
import org.opensearch.search.suggest.Suggest;

import java.io.IOException;

/**
 * The result of the query search
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class QuerySearchResult extends SearchPhaseResult {

    private final NativeQuerySearchResultSerializer serializer;

    private final boolean isNull;

    public QuerySearchResult() {
        this(false);
    }

    public QuerySearchResult(StreamInput in) throws IOException {
        super(in);
        isNull = in.readBoolean();
        serializer = new NativeQuerySearchResultSerializer(isNull);
        if (isNull == false) {
            this.contextId = new ShardSearchContextId(in);
            serializer.readQuerySearchResult(in, isNull);
            setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
            setRescoreDocIds(new RescoreDocIds(in));
        }
    }

    public QuerySearchResult(ShardSearchContextId contextId, SearchShardTarget shardTarget, ShardSearchRequest shardSearchRequest) {
        serializer = new NativeQuerySearchResultSerializer(false);
        this.contextId = contextId;
        setSearchShardTarget(shardTarget);
        isNull = false;
        setShardSearchRequest(shardSearchRequest);
    }

    private QuerySearchResult(boolean isNull) {
        this.isNull = isNull;
        serializer = new NativeQuerySearchResultSerializer(isNull);
    }

    /**
     * Returns an instance that contains no response.
     */
    public static QuerySearchResult nullInstance() {
        return new QuerySearchResult(true);
    }

    public NativeQuerySearchResultSerializer getSerializer() {
        return serializer;
    }

    /**
     * Returns true if the result doesn't contain any useful information.
     * It is used by the search action to avoid creating an empty response on
     * shard request that rewrites to match_no_docs.
     * <p>
     * TODO: Currently we need the concrete aggregators to build empty responses. This means that we cannot
     *       build an empty response in the coordinating node so we rely on this hack to ensure that at least one shard
     *       returns a valid empty response. We should move the ability to create empty responses to aggregation builders
     *       in order to allow building empty responses directly from the coordinating node.
     */
    public boolean isNull() {
        return isNull;
    }

    @Override
    public QuerySearchResult queryResult() {
        return this;
    }

    public void searchTimedOut(boolean searchTimedOut) {
        serializer.searchTimedOut(searchTimedOut);
    }

    public boolean searchTimedOut() {
        return serializer.searchTimedOut();
    }

    public void terminatedEarly(boolean terminatedEarly) {
        serializer.terminatedEarly(terminatedEarly);
    }

    public Boolean terminatedEarly() {
        return serializer.terminatedEarly();
    }

    public TopDocsAndMaxScore topDocs() {
        return serializer.topDocs();
    }

    /**
     * Returns <code>true</code> iff the top docs have already been consumed.
     */
    public boolean hasConsumedTopDocs() {
        return serializer.hasConsumedTopDocs();
    }

    /**
     * Returns and nulls out the top docs for this search results. This allows to free up memory once the top docs are consumed.
     * @throws IllegalStateException if the top docs have already been consumed.
     */
    public TopDocsAndMaxScore consumeTopDocs() {
        return serializer.consumeTopDocs();
    }

    public void topDocs(TopDocsAndMaxScore topDocs, DocValueFormat[] sortValueFormats) {
        serializer.topDocs(topDocs, sortValueFormats);
    }

    public DocValueFormat[] sortValueFormats() {
        return serializer.sortValueFormats();
    }

    /**
     * Returns <code>true</code> if this query result has unconsumed aggregations
     */
    public boolean hasAggs() {
        return serializer.hasAggs();
    }

    /**
     * Returns and nulls out the aggregation for this search results. This allows to free up memory once the aggregation is consumed.
     * @throws IllegalStateException if the aggregations have already been consumed.
     */
    public DelayableWriteable<InternalAggregations> consumeAggs() {
        return serializer.consumeAggs();
    }

    public void aggregations(InternalAggregations aggregations) {
        serializer.aggregations(aggregations);
    }

    public DelayableWriteable<InternalAggregations> aggregations() {
        return serializer.aggregations();
    }

    /**
     * Returns and nulls out the profiled results for this search, or potentially null if result was empty.
     * This allows to free up memory once the profiled result is consumed.
     * @throws IllegalStateException if the profiled result has already been consumed.
     */
    public ProfileShardResult consumeProfileResult() {
        if (serializer.profileResults() == null) {
            throw new IllegalStateException("profile results already consumed");
        }
        ProfileShardResult result = serializer.profileResults();
        NetworkTime newNetworkTime = new NetworkTime(
            this.getShardSearchRequest().getInboundNetworkTime(),
            this.getShardSearchRequest().getOutboundNetworkTime()
        );
        result.setNetworkTime(newNetworkTime);
        serializer.profileResults(null);
        return result;
    }

    public boolean hasProfileResults() {
        return serializer.hasProfileResults();
    }

    public void consumeAll() {
        if (hasProfileResults()) {
            consumeProfileResult();
        }
        serializer.consumeAll();
    }

    /**
     * Sets the finalized profiling results for this query
     * @param shardResults The finalized profile
     */
    public void profileResults(ProfileShardResult shardResults) {
        serializer.profileResults(shardResults);
    }

    public Suggest suggest() {
        return serializer.suggest();
    }

    public void suggest(Suggest suggest) {
        serializer.suggest(suggest);
    }

    public int from() {
        return serializer.from();
    }

    public QuerySearchResult from(int from) {
        serializer.from(from);
        return this;
    }

    /**
     * Returns the maximum size of this results top docs.
     */
    public int size() {
        return serializer.size();
    }

    public QuerySearchResult size(int size) {
        serializer.size(size);
        return this;
    }

    public long serviceTimeEWMA() {
        return serializer.serviceTimeEWMA();
    }

    public QuerySearchResult serviceTimeEWMA(long serviceTimeEWMA) {
        serializer.serviceTimeEWMA(serviceTimeEWMA);
        return this;
    }

    public int nodeQueueSize() {
        return serializer.nodeQueueSize();
    }

    public QuerySearchResult nodeQueueSize(int nodeQueueSize) {
        serializer.nodeQueueSize(nodeQueueSize);
        return this;
    }

    /**
     * Returns <code>true</code> if this result has any suggest score docs
     */
    public boolean hasSuggestHits() {
        return serializer.hasSuggestHits();
    }

    public boolean hasSearchContext() {
        return serializer.hasSearchContext();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isNull);
        if (isNull == false) {
            contextId.writeTo(out);
            serializer.writeQuerySearchResult(out);
            out.writeOptionalWriteable(getShardSearchRequest());
            getRescoreDocIds().writeTo(out);
        }
    }

    public TotalHits getTotalHits() {
        return serializer.getTotalHits();
    }

    public float getMaxScore() {
        return serializer.getMaxScore();
    }
}
