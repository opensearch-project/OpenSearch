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

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.io.stream.DelayableWriteable;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.RescoreDocIds;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.profile.NetworkTime;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.server.proto.QuerySearchResultProto;
import org.opensearch.server.proto.ShardSearchRequestProto;
import org.opensearch.server.proto.ShardSearchRequestProto.AliasFilter;
import org.opensearch.server.proto.ShardSearchRequestProto.ShardSearchRequest.SearchType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.opensearch.common.lucene.Lucene.readTopDocs;
import static org.opensearch.common.lucene.Lucene.writeTopDocs;

/**
 * The result of the query search
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class QuerySearchResult extends SearchPhaseResult {

    private int from;
    private int size;
    private TopDocsAndMaxScore topDocsAndMaxScore;
    private boolean hasScoreDocs;
    private TotalHits totalHits;
    private float maxScore = Float.NaN;
    private DocValueFormat[] sortValueFormats;
    /**
     * Aggregation results. We wrap them in
     * {@linkplain DelayableWriteable} because
     * {@link InternalAggregation} is usually made up of many small objects
     * which have a fairly high overhead in the JVM. So we delay deserializing
     * them until just before we need them.
     */
    private DelayableWriteable<InternalAggregations> aggregations;
    private boolean hasAggs;
    private Suggest suggest;
    private boolean searchTimedOut;
    private Boolean terminatedEarly = null;
    private ProfileShardResult profileShardResults;
    private boolean hasProfileResults;
    private long serviceTimeEWMA = -1;
    private int nodeQueueSize = -1;

    private final boolean isNull;

    private QuerySearchResultProto.QuerySearchResult querySearchResultProto;

    public QuerySearchResult() {
        this(false);
    }

    public QuerySearchResult(StreamInput in) throws IOException {
        super(in);
        isNull = in.readBoolean();
        if (isNull == false) {
            ShardSearchContextId id = new ShardSearchContextId(in);
            readFromWithId(id, in);
        }
    }

    public QuerySearchResult(InputStream in) throws IOException {
        this.querySearchResultProto = QuerySearchResultProto.QuerySearchResult.parseFrom(in);
        isNull = this.querySearchResultProto.getIsNull();
        if (!isNull) {
            this.contextId = new ShardSearchContextId(
                this.querySearchResultProto.getContextId().getSessionId(),
                this.querySearchResultProto.getContextId().getId()
            );
        }
    }

    public QuerySearchResult(ShardSearchContextId contextId, SearchShardTarget shardTarget, ShardSearchRequest shardSearchRequest) {
        this.contextId = contextId;
        setSearchShardTarget(shardTarget);
        isNull = false;
        setShardSearchRequest(shardSearchRequest);

        ShardSearchRequestProto.ShardId shardIdProto = ShardSearchRequestProto.ShardId.newBuilder()
            .setShardId(shardTarget.getShardId().getId())
            .setHashCode(shardTarget.getShardId().hashCode())
            .setIndexName(shardTarget.getShardId().getIndexName())
            .setIndexUUID(shardTarget.getShardId().getIndex().getUUID())
            .build();
        QuerySearchResultProto.SearchShardTarget.Builder searchShardTarget = QuerySearchResultProto.SearchShardTarget.newBuilder()
            .setNodeId(shardTarget.getNodeId())
            .setShardId(shardIdProto);
        ShardSearchRequestProto.ShardSearchContextId shardSearchContextId = ShardSearchRequestProto.ShardSearchContextId.newBuilder()
            .setSessionId(contextId.getSessionId())
            .setId(contextId.getId())
            .build();
        ShardSearchRequestProto.ShardSearchRequest.Builder shardSearchRequestProto = ShardSearchRequestProto.ShardSearchRequest
            .newBuilder();
        if (shardSearchRequest != null) {
            ShardSearchRequestProto.OriginalIndices.Builder originalIndices = ShardSearchRequestProto.OriginalIndices.newBuilder();
            if (shardSearchRequest.indices() != null) {
                for (String index : shardSearchRequest.indices()) {
                    originalIndices.addIndices(index);
                }
                originalIndices.setIndicesOptions(
                    ShardSearchRequestProto.OriginalIndices.IndicesOptions.newBuilder()
                        .setIgnoreUnavailable(shardSearchRequest.indicesOptions().ignoreUnavailable())
                        .setAllowNoIndices(shardSearchRequest.indicesOptions().allowNoIndices())
                        .setExpandWildcardsOpen(shardSearchRequest.indicesOptions().expandWildcardsOpen())
                        .setExpandWildcardsClosed(shardSearchRequest.indicesOptions().expandWildcardsClosed())
                        .setExpandWildcardsHidden(shardSearchRequest.indicesOptions().expandWildcardsHidden())
                        .setAllowAliasesToMultipleIndices(shardSearchRequest.indicesOptions().allowAliasesToMultipleIndices())
                        .setForbidClosedIndices(shardSearchRequest.indicesOptions().forbidClosedIndices())
                        .setIgnoreAliases(shardSearchRequest.indicesOptions().ignoreAliases())
                        .setIgnoreThrottled(shardSearchRequest.indicesOptions().ignoreThrottled())
                        .build()
                );
            }
            AliasFilter.Builder aliasFilter = AliasFilter.newBuilder();
            if (shardSearchRequest.getAliasFilter() != null) {
                for (int i = 0; i < shardSearchRequest.getAliasFilter().getAliases().length; i++) {
                    aliasFilter.addAliases(shardSearchRequest.getAliasFilter().getAliases()[i]);
                }
            }
            shardSearchRequestProto.setInboundNetworkTime(shardSearchRequest.getInboundNetworkTime())
                .setOutboundNetworkTime(shardSearchRequest.getOutboundNetworkTime())
                .setShardId(shardIdProto)
                .setAllowPartialSearchResults(shardSearchRequest.allowPartialSearchResults())
                .setNumberOfShards(shardSearchRequest.numberOfShards())
                .setReaderId(shardSearchContextId)
                .setOriginalIndices(originalIndices)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setAliasFilter(aliasFilter);
            if (shardSearchRequest.keepAlive() != null) {
                shardSearchRequestProto.setTimeValue(shardSearchRequest.keepAlive().getStringRep());
            }
        }

        if (shardTarget.getClusterAlias() != null) {
            searchShardTarget.setClusterAlias(shardTarget.getClusterAlias());
        }

        this.querySearchResultProto = QuerySearchResultProto.QuerySearchResult.newBuilder()
            .setContextId(shardSearchContextId)
            .setSearchShardTarget(searchShardTarget.build())
            .setSearchShardRequest(shardSearchRequestProto.build())
            .setHasAggs(false)
            .setIsNull(isNull)
            .build();
    }

    private QuerySearchResult(boolean isNull) {
        this.isNull = isNull;
    }

    /**
     * Returns an instance that contains no response.
     */
    public static QuerySearchResult nullInstance() {
        return new QuerySearchResult(true);
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
        this.searchTimedOut = searchTimedOut;
    }

    public boolean searchTimedOut() {
        return searchTimedOut;
    }

    public void terminatedEarly(boolean terminatedEarly) {
        this.terminatedEarly = terminatedEarly;
    }

    public Boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    public TopDocsAndMaxScore topDocs() {
        if (topDocsAndMaxScore == null && this.querySearchResultProto.getTopDocsAndMaxScore() == null) {
            throw new IllegalStateException("topDocs already consumed");
        }
        if (FeatureFlags.isEnabled(FeatureFlags.PROTOBUF_SETTING)) {
            ScoreDoc[] scoreDocs = new ScoreDoc[this.querySearchResultProto.getTopDocsAndMaxScore().getTopDocs().getScoreDocsCount()];
            for (int i = 0; i < scoreDocs.length; i++) {
                org.opensearch.server.proto.QuerySearchResultProto.QuerySearchResult.TopDocs.ScoreDoc scoreDoc = this.querySearchResultProto
                    .getTopDocsAndMaxScore()
                    .getTopDocs()
                    .getScoreDocsList()
                    .get(i);
                scoreDocs[i] = new ScoreDoc(scoreDoc.getDoc(), scoreDoc.getScore(), scoreDoc.getShardIndex());
            }
            TopDocs topDocsFromProtobuf = new TopDocs(
                new TotalHits(
                    this.querySearchResultProto.getTotalHits().getValue(),
                    Relation.valueOf(this.querySearchResultProto.getTotalHits().getRelation().toString())
                ),
                scoreDocs
            );

            TopDocsAndMaxScore topDocsFromProtobufAndMaxScore = new TopDocsAndMaxScore(
                topDocsFromProtobuf,
                this.querySearchResultProto.getMaxScore()
            );
            return topDocsFromProtobufAndMaxScore;
        }
        return topDocsAndMaxScore;
    }

    /**
     * Returns <code>true</code> iff the top docs have already been consumed.
     */
    public boolean hasConsumedTopDocs() {
        return topDocsAndMaxScore == null;
    }

    /**
     * Returns and nulls out the top docs for this search results. This allows to free up memory once the top docs are consumed.
     * @throws IllegalStateException if the top docs have already been consumed.
     */
    public TopDocsAndMaxScore consumeTopDocs() {
        TopDocsAndMaxScore topDocsAndMaxScore = this.topDocsAndMaxScore;
        if (topDocsAndMaxScore == null) {
            throw new IllegalStateException("topDocs already consumed");
        }
        this.topDocsAndMaxScore = null;
        return topDocsAndMaxScore;
    }

    public void topDocs(TopDocsAndMaxScore topDocs, DocValueFormat[] sortValueFormats) {
        setTopDocs(topDocs);
        if (topDocs.topDocs.scoreDocs.length > 0 && topDocs.topDocs.scoreDocs[0] instanceof FieldDoc) {
            int numFields = ((FieldDoc) topDocs.topDocs.scoreDocs[0]).fields.length;
            if (numFields != sortValueFormats.length) {
                throw new IllegalArgumentException(
                    "The number of sort fields does not match: " + numFields + " != " + sortValueFormats.length
                );
            }
        }
        this.sortValueFormats = sortValueFormats;
    }

    private void setTopDocs(TopDocsAndMaxScore topDocsAndMaxScore) {
        this.topDocsAndMaxScore = topDocsAndMaxScore;
        this.totalHits = topDocsAndMaxScore.topDocs.totalHits;
        this.maxScore = topDocsAndMaxScore.maxScore;
        this.hasScoreDocs = topDocsAndMaxScore.topDocs.scoreDocs.length > 0;
    }

    public DocValueFormat[] sortValueFormats() {
        return sortValueFormats;
    }

    /**
     * Returns <code>true</code> if this query result has unconsumed aggregations
     */
    public boolean hasAggs() {
        return hasAggs;
    }

    /**
     * Returns and nulls out the aggregation for this search results. This allows to free up memory once the aggregation is consumed.
     * @throws IllegalStateException if the aggregations have already been consumed.
     */
    public DelayableWriteable<InternalAggregations> consumeAggs() {
        if (aggregations == null) {
            throw new IllegalStateException("aggs already consumed");
        }
        DelayableWriteable<InternalAggregations> aggs = aggregations;
        aggregations = null;
        return aggs;
    }

    public void aggregations(InternalAggregations aggregations) {
        this.aggregations = aggregations == null ? null : DelayableWriteable.referencing(aggregations);
        hasAggs = aggregations != null;
    }

    public DelayableWriteable<InternalAggregations> aggregations() {
        return aggregations;
    }

    /**
     * Returns and nulls out the profiled results for this search, or potentially null if result was empty.
     * This allows to free up memory once the profiled result is consumed.
     * @throws IllegalStateException if the profiled result has already been consumed.
     */
    public ProfileShardResult consumeProfileResult() {
        if (profileShardResults == null) {
            throw new IllegalStateException("profile results already consumed");
        }
        ProfileShardResult result = profileShardResults;
        NetworkTime newNetworkTime = new NetworkTime(
            this.getShardSearchRequest().getInboundNetworkTime(),
            this.getShardSearchRequest().getOutboundNetworkTime()
        );
        result.setNetworkTime(newNetworkTime);
        profileShardResults = null;
        return result;
    }

    public boolean hasProfileResults() {
        return hasProfileResults;
    }

    public void consumeAll() {
        if (hasProfileResults()) {
            consumeProfileResult();
        }
        if (hasConsumedTopDocs() == false) {
            consumeTopDocs();
        }
        if (hasAggs()) {
            consumeAggs();
        }
    }

    /**
     * Sets the finalized profiling results for this query
     * @param shardResults The finalized profile
     */
    public void profileResults(ProfileShardResult shardResults) {
        this.profileShardResults = shardResults;
        hasProfileResults = shardResults != null;
    }

    public Suggest suggest() {
        return suggest;
    }

    public void suggest(Suggest suggest) {
        this.suggest = suggest;
    }

    public int from() {
        if (FeatureFlags.isEnabled(FeatureFlags.PROTOBUF_SETTING)) {
            return this.querySearchResultProto.getFrom();
        }
        return from;
    }

    public QuerySearchResult from(int from) {
        this.from = from;
        return this;
    }

    /**
     * Returns the maximum size of this results top docs.
     */
    public int size() {
        if (FeatureFlags.isEnabled(FeatureFlags.PROTOBUF_SETTING)) {
            return this.querySearchResultProto.getSize();
        }
        return size;
    }

    public QuerySearchResult size(int size) {
        this.size = size;
        return this;
    }

    public long serviceTimeEWMA() {
        return this.serviceTimeEWMA;
    }

    public QuerySearchResult serviceTimeEWMA(long serviceTimeEWMA) {
        this.serviceTimeEWMA = serviceTimeEWMA;
        return this;
    }

    public int nodeQueueSize() {
        return this.nodeQueueSize;
    }

    public QuerySearchResult nodeQueueSize(int nodeQueueSize) {
        this.nodeQueueSize = nodeQueueSize;
        return this;
    }

    /**
     * Returns <code>true</code> if this result has any suggest score docs
     */
    public boolean hasSuggestHits() {
        return (suggest != null && suggest.hasScoreDocs());
    }

    public boolean hasSearchContext() {
        return hasScoreDocs || hasSuggestHits();
    }

    public void readFromWithId(ShardSearchContextId id, StreamInput in) throws IOException {
        this.contextId = id;
        from = in.readVInt();
        size = in.readVInt();
        int numSortFieldsPlus1 = in.readVInt();
        if (numSortFieldsPlus1 == 0) {
            sortValueFormats = null;
        } else {
            sortValueFormats = new DocValueFormat[numSortFieldsPlus1 - 1];
            for (int i = 0; i < sortValueFormats.length; ++i) {
                sortValueFormats[i] = in.readNamedWriteable(DocValueFormat.class);
            }
        }
        setTopDocs(readTopDocs(in));
        if (hasAggs = in.readBoolean()) {
            aggregations = DelayableWriteable.delayed(InternalAggregations::readFrom, in);
        }
        if (in.readBoolean()) {
            suggest = new Suggest(in);
        }
        searchTimedOut = in.readBoolean();
        terminatedEarly = in.readOptionalBoolean();
        profileShardResults = in.readOptionalWriteable(ProfileShardResult::new);
        hasProfileResults = profileShardResults != null;
        serviceTimeEWMA = in.readZLong();
        nodeQueueSize = in.readInt();
        setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
        setRescoreDocIds(new RescoreDocIds(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isNull);
        if (isNull == false) {
            contextId.writeTo(out);
            writeToNoId(out);
        }
    }

    public void writeTo(OutputStream out) throws IOException {
        if (!isNull) {
            out.write(this.querySearchResultProto.toByteArray());
        }
    }

    public void writeToNoId(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeVInt(size);
        if (sortValueFormats == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(1 + sortValueFormats.length);
            for (int i = 0; i < sortValueFormats.length; ++i) {
                out.writeNamedWriteable(sortValueFormats[i]);
            }
        }
        writeTopDocs(out, topDocsAndMaxScore);
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(searchTimedOut);
        out.writeOptionalBoolean(terminatedEarly);
        out.writeOptionalWriteable(profileShardResults);
        out.writeZLong(serviceTimeEWMA);
        out.writeInt(nodeQueueSize);
        out.writeOptionalWriteable(getShardSearchRequest());
        getRescoreDocIds().writeTo(out);
    }

    public TotalHits getTotalHits() {
        return totalHits;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public QuerySearchResultProto.QuerySearchResult response() {
        return this.querySearchResultProto;
    }
}
