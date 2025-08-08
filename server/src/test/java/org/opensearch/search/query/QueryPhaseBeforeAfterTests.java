/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Query;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Before/After comparison showing the optimization fixes the regression
 */
public class QueryPhaseBeforeAfterTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 10_000_000;  // 10 million iterations for measurable results

    /**
     * Shows the before (unoptimized) vs after (optimized) performance
     */
    public void testBeforeAfterOptimization() throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        when(searchContext.searcher()).thenReturn(searcher);
        when(searchContext.query()).thenReturn(query);

        // Create implementations
        QueryPhaseSearcher beforeOptimization = createUnoptimizedNeuralSearch();
        QueryPhaseSearcher afterOptimization = createOptimizedNeuralSearch();

        // Warmup
        warmup(searchContext, searcher, query, beforeOptimization, afterOptimization);

        // Measure
        long beforeTime = measureImplementation("BEFORE (unoptimized)", beforeOptimization, searchContext, searcher, query);
        long afterTime = measureImplementation("AFTER (optimized)", afterOptimization, searchContext, searcher, query);

        // Calculate improvement
        double improvementPercent = ((double) (beforeTime - afterTime) / beforeTime) * 100;

        // Verify optimization provides improvement
        // We expect the optimized version to be faster in most runs
        // though exact improvement varies due to JVM optimizations
        assertTrue("Optimized version should be faster or equal", afterTime <= beforeTime);
    }

    private void warmup(SearchContext searchContext, ContextIndexSearcher searcher, Query query, QueryPhaseSearcher... impls)
        throws IOException {
        for (int i = 0; i < 100_000; i++) {  // More warmup iterations
            for (QueryPhaseSearcher impl : impls) {
                LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
                impl.searchWith(searchContext, searcher, query, collectors, false, false);
            }
        }
    }

    private long measureImplementation(
        String name,
        QueryPhaseSearcher impl,
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query
    ) throws IOException {
        System.gc();

        long startTime = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
            impl.searchWith(searchContext, searcher, query, collectors, false, false);
        }
        long totalTime = System.nanoTime() - startTime;
        return totalTime;
    }

    private void simulateWork() {
        // Simulate very light work - this is where overhead is most noticeable
        // In neural-search, they were doing minimal work which is why
        // the template pattern overhead was significant (10-15%)
        long sum = System.nanoTime() % 100;
        // Prevent optimization
        if (sum < 0) {
            throw new RuntimeException("Never happens");
        }
    }

    /**
     * Simulates the BEFORE state: Always goes through template pattern
     */
    private QueryPhaseSearcher createUnoptimizedNeuralSearch() {
        return new UnoptimizedAbstractQueryPhaseSearcher() {
            @Override
            protected boolean doSearchWith(
                SearchContext sc,
                ContextIndexSearcher s,
                Query q,
                LinkedList<QueryCollectorContext> c,
                boolean hasFilter,
                boolean hasTimeout
            ) throws IOException {
                c.add(QueryCollectorContext.EMPTY_CONTEXT);
                // Simulate some minimal work that would happen in real search
                simulateWork();
                return false;
            }

            @Override
            protected QueryCollectorContext getQueryCollectorContext(SearchContext searchContext, boolean hasFilterCollector) {
                return QueryCollectorContext.EMPTY_CONTEXT;
            }
        };
    }

    /**
     * Represents the AFTER state: Our optimized implementation
     */
    private QueryPhaseSearcher createOptimizedNeuralSearch() {
        return new QueryPhase.DefaultQueryPhaseSearcher() {
            @Override
            protected boolean searchWithCollector(
                SearchContext sc,
                ContextIndexSearcher s,
                Query q,
                LinkedList<QueryCollectorContext> c,
                boolean hasFilter,
                boolean hasTimeout
            ) throws IOException {
                c.add(QueryCollectorContext.EMPTY_CONTEXT);
                // Simulate some minimal work that would happen in real search
                simulateWork();
                return false;
            }

            @Override
            protected QueryCollectorContext getQueryCollectorContext(SearchContext searchContext, boolean hasFilterCollector) {
                return QueryCollectorContext.EMPTY_CONTEXT;
            }
        };
    }

    /**
     * Simulates the unoptimized template pattern (BEFORE state)
     */
    private abstract static class UnoptimizedAbstractQueryPhaseSearcher implements QueryPhaseSearcher {
        @Override
        public final boolean searchWith(
            SearchContext sc,
            ContextIndexSearcher s,
            Query q,
            LinkedList<QueryCollectorContext> c,
            boolean hasFilter,
            boolean hasTimeout
        ) throws IOException {
            // ALWAYS executes template pattern, even with no listeners
            List<QueryPhaseListener> listeners = queryPhaseListeners();

            for (QueryPhaseListener listener : listeners) {
                listener.beforeCollection(sc);
            }

            boolean result = doSearchWith(sc, s, q, c, hasFilter, hasTimeout);

            for (QueryPhaseListener listener : listeners) {
                listener.afterCollection(sc);
            }

            return result;
        }

        protected abstract boolean doSearchWith(
            SearchContext sc,
            ContextIndexSearcher s,
            Query q,
            LinkedList<QueryCollectorContext> c,
            boolean hasFilter,
            boolean hasTimeout
        ) throws IOException;

        @Override
        public List<QueryPhaseListener> queryPhaseListeners() {
            return Collections.emptyList();
        }

        @Override
        public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
            return null;
        }

        protected abstract QueryCollectorContext getQueryCollectorContext(SearchContext searchContext, boolean hasFilterCollector);
    }
}
