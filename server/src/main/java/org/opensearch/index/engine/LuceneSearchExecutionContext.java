/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

/**
 * Lucene-specific {@link SearchExecutionContext} implementation.
 * <p>
 * Wraps the existing {@link SearchContext} to bridge the legacy Lucene execution
 * model with the new engine-agnostic interface. The wrapped SearchContext
 * holds all Lucene-specific state (ContextIndexSearcher, parsed query, etc.)
 * while this class exposes only the engine-agnostic contract.
 * <p>
 * Also provides access to the underlying SearchContext for Lucene-specific
 * operations like QueryPhase/FetchPhase execution.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchExecutionContext implements SearchExecutionContext {

    private final SearchContext searchContext;

    public LuceneSearchExecutionContext(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    /**
     * Access the underlying Lucene SearchContext for engine-internal operations.
     */
    public SearchContext getSearchContext() {
        return searchContext;
    }

    @Override
    public ShardSearchContextId id() {
        return searchContext.id();
    }

    @Override
    public ShardSearchRequest request() {
        return searchContext.request();
    }

    @Override
    public SearchShardTarget shardTarget() {
        return searchContext.shardTarget();
    }

    @Override
    public QuerySearchResult queryResult() {
        return searchContext.queryResult();
    }

    @Override
    public FetchSearchResult fetchResult() {
        return searchContext.fetchResult();
    }

    @Override
    public int from() {
        return searchContext.from();
    }

    @Override
    public int size() {
        return searchContext.size();
    }

    @Override
    public int[] docIdsToLoad() {
        return searchContext.docIdsToLoad();
    }

    @Override
    public void docIdsToLoad(int[] docIds, int from, int size) {
        searchContext.docIdsToLoad(docIds, from, size);
    }

    @Override
    public void close() throws IOException {
        searchContext.close();
    }
}
