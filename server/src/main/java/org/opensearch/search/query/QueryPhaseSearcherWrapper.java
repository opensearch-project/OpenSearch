/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Wrapper class for QueryPhaseSearcher that handles path selection for concurrent vs
 * non-concurrent search query phase and aggregation processor.
 *
 * @opensearch.internal
 */
public class QueryPhaseSearcherWrapper implements QueryPhaseSearcher {
    private static final Logger LOGGER = LogManager.getLogger(QueryPhaseSearcherWrapper.class);
    private final QueryPhaseSearcher defaultQueryPhaseSearcher;
    private final QueryPhaseSearcher concurrentQueryPhaseSearcher;

    public QueryPhaseSearcherWrapper() {
        this.defaultQueryPhaseSearcher = new QueryPhase.DefaultQueryPhaseSearcher();
        this.concurrentQueryPhaseSearcher = new ConcurrentQueryPhaseSearcher();
    }

    /**
     * Perform search using {@link CollectorManager}
     *
     * @param searchContext      search context
     * @param searcher           context index searcher
     * @param query              query
     * @param hasTimeout         "true" if timeout was set, "false" otherwise
     * @return is rescoring required or not
     * @throws java.io.IOException IOException
     */
    @Override
    public boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        // if (searchContext.shouldUseConcurrentSearch()) {
        // return concurrentQueryPhaseSearcher.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        // } else {
        // return defaultQueryPhaseSearcher.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        // }
        //
        return defaultQueryPhaseSearcher.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    /**
     * {@link AggregationProcessor} to use to setup and post process aggregation related collectors during search request
     * @param searchContext search context
     * @return {@link AggregationProcessor} to use
     */
    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        // if (searchContext.shouldUseConcurrentSearch()) {
        // return concurrentQueryPhaseSearcher.aggregationProcessor(searchContext);
        // } else {
        // return defaultQueryPhaseSearcher.aggregationProcessor(searchContext);
        // }
        return defaultQueryPhaseSearcher.aggregationProcessor(searchContext);
    }
}
