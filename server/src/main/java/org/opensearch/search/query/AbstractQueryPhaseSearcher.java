/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.opensearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;

/**
 * Abstract base class for QueryPhaseSearcher implementations that provides
 * extension hook execution logic using the template pattern.
 *
 * @opensearch.internal
 */
@InternalApi
public abstract class AbstractQueryPhaseSearcher implements QueryPhaseSearcher {

    @Override
    public final boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        List<QueryPhaseListener> listeners = queryPhaseListeners();

        // Execute beforeCollection listeners
        for (QueryPhaseListener listener : listeners) {
            listener.beforeCollection(searchContext);
        }

        boolean shouldRescore = doSearchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        // Execute afterCollection listeners
        for (QueryPhaseListener listener : listeners) {
            listener.afterCollection(searchContext);
        }
        return shouldRescore;
    }

    /**
     * Template method for actual search implementation.
     * Subclasses must implement this to define their specific search behavior.
     */
    protected abstract boolean doSearchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException;

    /**
     * Common method to create QueryCollectorContext that can be used by all implementations.
     */
    protected QueryCollectorContext getQueryCollectorContext(SearchContext searchContext, boolean hasFilterCollector) throws IOException {
        // create the top docs collector last when the other collectors are known
        final Optional<QueryCollectorContext> queryCollectorContextOpt = QueryCollectorContextSpecRegistry.getQueryCollectorContextSpec(
            searchContext,
            new QueryCollectorArguments.Builder().hasFilterCollector(hasFilterCollector).build()
        ).map(queryCollectorContextSpec -> new QueryCollectorContext(queryCollectorContextSpec.getContextName()) {
            @Override
            Collector create(Collector in) throws IOException {
                return queryCollectorContextSpec.create(in);
            }

            @Override
            CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
                return queryCollectorContextSpec.createManager(in);
            }

            @Override
            void postProcess(QuerySearchResult result) throws IOException {
                queryCollectorContextSpec.postProcess(result);
            }
        });
        if (queryCollectorContextOpt.isPresent()) {
            return queryCollectorContextOpt.get();
        } else {
            return createTopDocsCollectorContext(searchContext, hasFilterCollector);
        }
    }
}
