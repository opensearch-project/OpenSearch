/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.DefaultAggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * The extension point which allows to plug in custom search implementation to be
 * used at {@link QueryPhase}.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.0.0")
public interface QueryPhaseSearcher {
    /**
     * Perform search using {@link CollectorManager}
     * @param searchContext search context
     * @param searcher context index searcher
     * @param query query
     * @param hasTimeout "true" if timeout was set, "false" otherwise
     * @return is rescoring required or not
     * @throws IOException IOException
     */
    boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException;

    /**
     * {@link AggregationProcessor} to use to setup and post process aggregation related collectors during search request
     * @param searchContext search context
     * @return {@link AggregationProcessor} to use
     */
    default AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        return new DefaultAggregationProcessor();
    }

    /**
     * Get the list of query phase listeners that should be executed before and after score collection.
     * @return list of query phase listeners, empty list if none
     */
    default List<QueryPhaseListener> queryPhaseListeners() {
        return Collections.emptyList();
    }
}
