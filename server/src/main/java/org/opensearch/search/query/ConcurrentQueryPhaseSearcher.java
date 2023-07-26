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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.ConcurrentAggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.ProfileCollectorManager;
import org.opensearch.search.query.QueryPhase.DefaultQueryPhaseSearcher;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;

import static org.opensearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;

/**
 * The implementation of the {@link QueryPhaseSearcher} which attempts to use concurrent
 * search of Apache Lucene segments if it has been enabled.
 */
public class ConcurrentQueryPhaseSearcher extends DefaultQueryPhaseSearcher {
    private static final Logger LOGGER = LogManager.getLogger(ConcurrentQueryPhaseSearcher.class);
    private final AggregationProcessor aggregationProcessor = new ConcurrentAggregationProcessor();

    /**
     * Default constructor
     */
    public ConcurrentQueryPhaseSearcher() {}

    @Override
    protected boolean searchWithCollector(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        return searchWithCollectorManager(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    private static boolean searchWithCollectorManager(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectorContexts,
        boolean hasFilterCollector,
        boolean timeoutSet
    ) throws IOException {
        // create the top docs collector last when the other collectors are known
        final TopDocsCollectorContext topDocsFactory = createTopDocsCollectorContext(searchContext, hasFilterCollector);
        // add the top docs collector, the first collector context in the chain
        collectorContexts.addFirst(topDocsFactory);

        final QuerySearchResult queryResult = searchContext.queryResult();
        final CollectorManager<?, ReduceableSearchResult> collectorManager;

        if (searchContext.getProfilers() != null) {
            final ProfileCollectorManager<? extends Collector, ReduceableSearchResult> profileCollectorManager =
                QueryCollectorManagerContext.createQueryCollectorManagerWithProfiler(collectorContexts);
            searchContext.getProfilers().getCurrentQueryProfiler().setCollector(profileCollectorManager);
            collectorManager = profileCollectorManager;
        } else {
            // Create collector manager tree
            collectorManager = QueryCollectorManagerContext.createQueryCollectorManager(collectorContexts);
        }

        try {
            final ReduceableSearchResult result = searcher.search(query, collectorManager);
            result.reduce(queryResult);
        } catch (RuntimeException re) {
            rethrowCauseIfPossible(re, searchContext);
        }
        if (searchContext.isSearchTimedOut()) {
            assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
            if (searchContext.request().allowPartialSearchResults() == false) {
                throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Time exceeded");
            }
            queryResult.searchTimedOut(true);
        }
        if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
            queryResult.terminatedEarly(false);
        }

        return topDocsFactory.shouldRescore();
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        return aggregationProcessor;
    }

    private static <T extends Exception> void rethrowCauseIfPossible(RuntimeException re, SearchContext searchContext) throws T {
        // Rethrow exception if cause is null
        if (re.getCause() == null) {
            throw re;
        }

        // Unwrap the RuntimeException and ExecutionException from Lucene concurrent search method and rethrow
        if (re.getCause() instanceof ExecutionException || re.getCause() instanceof InterruptedException) {
            Throwable t = re.getCause();
            if (t.getCause() != null) {
                throw (T) t.getCause();
            }
        }

        // Rethrow any unexpected exception types
        throw new QueryPhaseExecutionException(
            searchContext.shardTarget(),
            "Failed to execute concurrent segment search thread",
            re.getCause()
        );
    }
}
