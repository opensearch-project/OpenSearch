/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.Map;

/**
 * DataFusion-specific search execution context.
 * <p>
 * Implements {@link SearchExecutionContext} directly — no need to extend
 * the Lucene-centric {@code SearchContext} with its ~50 null methods.
 * <p>
 * Carries the DataFusion query plan, engine searcher, optional {@link IndexFilterTree},
 * and columnar results.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionContext implements SearchExecutionContext {

    private final ShardSearchContextId id;
    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;
    private final DatafusionSearcher engineSearcher;

    private DatafusionQuery datafusionQuery;
    private IndexFilterTree filterTree;
    private Map<String, Object[]> dfResults;
    private int[] docIdsToLoad;
    private int docIdsToLoadFrom;
    private int docIdsToLoadSize;

    public DatafusionContext(
        ShardSearchContextId id,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        DatafusionSearcher engineSearcher
    ) {
        this.id = id;
        this.request = request;
        this.shardTarget = shardTarget;
        this.engineSearcher = engineSearcher;
        this.queryResult = new QuerySearchResult(id, shardTarget, request);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
    }

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
        return request.source() != null ? request.source().from() : 0;
    }

    @Override
    public int size() {
        return request.source() != null ? request.source().size() : 10;
    }

    @Override
    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    @Override
    public void docIdsToLoad(int[] docIds, int from, int size) {
        this.docIdsToLoad = docIds;
        this.docIdsToLoadFrom = from;
        this.docIdsToLoadSize = size;
    }

    @Override
    public void close() throws IOException {
        try {
            if (filterTree != null) {
                filterTree.close();
            }
        } finally {
            engineSearcher.close();
        }
    }

    // --- DataFusion-specific ---

    public DatafusionSearcher getEngineSearcher() {
        return engineSearcher;
    }

    public DatafusionQuery getDatafusionQuery() {
        return datafusionQuery;
    }

    public void setDatafusionQuery(DatafusionQuery query) {
        this.datafusionQuery = query;
    }

    /**
     * Returns the optional filter tree for indexed parquet queries.
     * {@code null} indicates a pure parquet query with no external index involvement.
     */
    public IndexFilterTree getFilterTree() {
        return filterTree;
    }

    /**
     * Sets the filter tree for indexed parquet queries.
     */
    public void setFilterTree(IndexFilterTree filterTree) {
        this.filterTree = filterTree;
    }

    public Map<String, Object[]> getDatafusionResults() {
        return dfResults;
    }

    public void setDatafusionResults(Map<String, Object[]> dfResults) {
        this.dfResults = dfResults;
    }
}
