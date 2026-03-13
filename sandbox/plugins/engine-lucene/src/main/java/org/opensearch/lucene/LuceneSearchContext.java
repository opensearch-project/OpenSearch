/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

/**
 * Lucene-specific search execution context.
 * <p>
 * Input: a Lucene {@link Query}.
 * Output: a registered Weight pointer + segment metadata that Rust
 * uses for JNI callbacks to stream bitsets per partition range.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchContext implements SearchExecutionContext {

    private final ShardSearchContextId id;
    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;

    private Query query;
    private int[] docIdsToLoad;

    // Populated by LuceneEngineSearcher.search()
    private long weightPointer;
    private int segmentCount;
    private int[] segmentMaxDocs;

    public LuceneSearchContext(ShardSearchContextId id, ShardSearchRequest request, SearchShardTarget shardTarget) {
        this.id = id;
        this.request = request;
        this.shardTarget = shardTarget;
        this.queryResult = new QuerySearchResult(id, shardTarget, request);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
    }

    // ---- Input ----
    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    // ---- Output: Weight pointer for JNI callbacks ----
    public long getWeightPointer() {
        return weightPointer;
    }

    public void setWeightPointer(long weightPointer) {
        this.weightPointer = weightPointer;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public void setSegmentCount(int segmentCount) {
        this.segmentCount = segmentCount;
    }

    public int[] getSegmentMaxDocs() {
        return segmentMaxDocs;
    }

    public void setSegmentMaxDocs(int[] segmentMaxDocs) {
        this.segmentMaxDocs = segmentMaxDocs;
    }

    // ---- SearchExecutionContext ----
    @Override
    public ShardSearchContextId id() {
        return id;
    }

    @Override
    public ShardSearchRequest request() {
        return request;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return shardTarget;
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
    public int from() {
        return request != null && request.source() != null ? request.source().from() : 0;
    }

    @Override
    public int size() {
        return request != null && request.source() != null ? request.source().size() : 10;
    }

    @Override
    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    @Override
    public void docIdsToLoad(int[] docIds, int from, int size) {
        this.docIdsToLoad = docIds;
    }

    @Override
    public void close() throws IOException {
        // Release the registered Weight when context is closed
        if (weightPointer != 0) {
            LuceneEngineSearcher.releaseWeight(weightPointer);
            weightPointer = 0;
        }
    }
}
