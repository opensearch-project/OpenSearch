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
import org.opensearch.OpenSearchException;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.ConcurrentAggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.ProfileCollectorManager;
import org.opensearch.search.query.QueryPhase.DefaultQueryPhaseSearcher;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

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
        QueryCollectorContext queryCollectorContext,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        return searchWithCollectorManager(
            searchContext,
            searcher,
            query,
            collectors,
            queryCollectorContext,
            hasFilterCollector,
            hasTimeout
        );
    }

    private static boolean searchWithCollectorManager(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectorContexts,
        QueryCollectorContext queryCollectorContext,
        boolean hasFilterCollector,
        boolean timeoutSet
    ) throws IOException {
        // add the passed collector, the first collector context in the chain
        collectorContexts.addFirst(Objects.requireNonNull(queryCollectorContext));

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
            // Time the concurrent search execution (includes slice scheduling and execution within Lucene)
            final long searchStartNanos = System.nanoTime();
            final ReduceableSearchResult result = searcher.search(query, collectorManager);
            final long searchEndNanos = System.nanoTime();

            // Time the slice result aggregation (reduce step)
            final long reduceStartNanos = System.nanoTime();
            result.reduce(queryResult);
            final long reduceEndNanos = System.nanoTime();

            // Record concurrent segment search metrics
            // slice_creation is captured inside ContextIndexSearcher.slices()
            long sliceCreationNanos = searcher.getSliceCreationNanos();
            if (sliceCreationNanos > 0) {
                queryResult.recordShardTiming("slice_creation", sliceCreationNanos);
            }

            // slice_scheduling: time from search start to after slice creation is the scheduling overhead
            // This approximates the time to submit slices to the executor
            // (Lucene internally creates slices via slices() then submits tasks to the executor)
            long totalSearchNanos = Math.max(0, searchEndNanos - searchStartNanos);
            long sliceMaxExecNanos = searcher.getSliceMaxExecutionNanos();
            long sliceSchedulingNanos = Math.max(0, totalSearchNanos - sliceMaxExecNanos);
            if (sliceSchedulingNanos > 0) {
                queryResult.recordShardTiming("slice_scheduling", sliceSchedulingNanos);
            }

            // slice_max_execution and slice_min_execution captured in ContextIndexSearcher.search(partitions...)
            if (sliceMaxExecNanos > 0) {
                queryResult.recordShardTiming("slice_max_execution", sliceMaxExecNanos);
            }
            long sliceMinExecNanos = searcher.getSliceMinExecutionNanos();
            if (sliceMinExecNanos > 0) {
                queryResult.recordShardTiming("slice_min_execution", sliceMinExecNanos);
            }

            // slice_result_aggregation: time to merge slice results via reduce
            long sliceResultAggregationNanos = Math.max(0, reduceEndNanos - reduceStartNanos);
            if (sliceResultAggregationNanos > 0) {
                queryResult.recordShardTiming("slice_result_aggregation", sliceResultAggregationNanos);
            }

            // Record absolute start offsets for timeline positioning in the breakdown chart
            final long requestStartNanos = searchContext.request().getRequestStartNanos();
            if (requestStartNanos > 0) {
                // slice_creation starts at the beginning of the searcher.search() call
                if (sliceCreationNanos > 0) {
                    queryResult.recordShardTiming("slice_creation_start", Math.max(0, searchStartNanos - requestStartNanos));
                }
                // slice_scheduling starts after slice creation completes
                if (sliceSchedulingNanos > 0) {
                    long sliceSchedulingStartNanos = searchStartNanos + sliceCreationNanos;
                    queryResult.recordShardTiming("slice_scheduling_start", Math.max(0, sliceSchedulingStartNanos - requestStartNanos));
                }
                // slice execution starts after scheduling (slices run concurrently after being submitted)
                long sliceExecStartNanos = searchStartNanos + sliceCreationNanos + sliceSchedulingNanos;
                if (sliceMaxExecNanos > 0) {
                    queryResult.recordShardTiming("slice_max_execution_start", Math.max(0, sliceExecStartNanos - requestStartNanos));
                }
                if (sliceMinExecNanos > 0) {
                    queryResult.recordShardTiming("slice_min_execution_start", Math.max(0, sliceExecStartNanos - requestStartNanos));
                }
                // slice_result_aggregation starts at reduceStartNanos
                if (sliceResultAggregationNanos > 0) {
                    queryResult.recordShardTiming(
                        "slice_result_aggregation_start",
                        Math.max(0, reduceStartNanos - requestStartNanos)
                    );
                }
            }
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

        if (queryCollectorContext instanceof RescoringQueryCollectorContext rescoringContext) {
            return rescoringContext.shouldRescore();
        }
        return false;
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        return aggregationProcessor;
    }

    private static <T extends Exception> void rethrowCauseIfPossible(RuntimeException re, SearchContext searchContext) throws T {
        // Rethrow exception if cause is null or if it's an instance of OpenSearchException
        if (re.getCause() == null || re instanceof OpenSearchException) {
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
