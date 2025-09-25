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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.Booleans;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.concurrent.EWMATrackingThreadPoolExecutor;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lucene.queries.SearchAfterSortedDocQuery;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchContextSourcePrinter;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.DefaultAggregationProcessor;
import org.opensearch.search.aggregations.GlobalAggCollectorManager;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.profile.query.InternalProfileCollector;
import org.opensearch.search.rescore.RescoreProcessor;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.suggest.SuggestProcessor;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.opensearch.search.query.QueryCollectorContext.createEarlyTerminationCollectorContext;
import static org.opensearch.search.query.QueryCollectorContext.createFilteredCollectorContext;
import static org.opensearch.search.query.QueryCollectorContext.createMinScoreCollectorContext;
import static org.opensearch.search.query.QueryCollectorContext.createMultiCollectorContext;
import static org.opensearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;

/**
 * Query phase of a search request, used to run the query and get back from each shard information about the matching documents
 * (document ids and score or sort criteria) so that matches can be reduced on the coordinating node
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class QueryPhase {
    private static final Logger LOGGER = LogManager.getLogger(QueryPhase.class);
    // TODO: remove this property
    public static final boolean SYS_PROP_REWRITE_SORT = Booleans.parseBoolean(System.getProperty("opensearch.search.rewrite_sort", "true"));
    public static final QueryPhaseSearcher DEFAULT_QUERY_PHASE_SEARCHER = new DefaultQueryPhaseSearcher();
    private final QueryPhaseSearcher queryPhaseSearcher;
    private final SuggestProcessor suggestProcessor;
    private final RescoreProcessor rescoreProcessor;

    public QueryPhase() {
        this(DEFAULT_QUERY_PHASE_SEARCHER);
    }

    public QueryPhase(QueryPhaseSearcher queryPhaseSearcher) {
        this.queryPhaseSearcher = Objects.requireNonNull(queryPhaseSearcher, "QueryPhaseSearcher is required");
        this.suggestProcessor = new SuggestProcessor();
        this.rescoreProcessor = new RescoreProcessor();
    }

    public void preProcess(SearchContext context) {
        final Runnable cancellation;
        if (context.lowLevelCancellation()) {
            cancellation = context.searcher().addQueryCancellation(() -> {
                SearchShardTask task = context.getTask();
                if (task != null && task.isCancelled()) {
                    throw new TaskCancelledException("cancelled task with reason: " + task.getReasonCancelled());
                }
            });
        } else {
            cancellation = null;
        }
        try {
            context.preProcess(true);
        } finally {
            if (cancellation != null) {
                context.searcher().removeQueryCancellation(cancellation);
            }
        }
    }

    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        if (searchContext.hasOnlySuggest()) {
            suggestProcessor.process(searchContext);
            searchContext.queryResult()
                .topDocs(
                    new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS), Float.NaN),
                    new DocValueFormat[0]
                );
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(searchContext));
        }

        // Keeping AggregationProcessor and preProcess uncommented since it builds aggregation nesting
        final AggregationProcessor aggregationProcessor = queryPhaseSearcher.aggregationProcessor(searchContext);
        // Pre-process aggregations as late as possible. In the case of a DFS_Q_T_F
        // request, preProcess is called on the DFS phase phase, this is why we pre-process them
        // here to make sure it happens during the QUERY phase
        aggregationProcessor.preProcess(searchContext);

        searchContext.queryResult()
            .topDocs(
                new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS), Float.NaN),
                new DocValueFormat[0]
            );

        // boolean rescore = executeInternal(searchContext, queryPhaseSearcher);

        // if (rescore) { // only if we do a regular search
        // rescoreProcessor.process(searchContext);
        // }
        // suggestProcessor.process(searchContext);
        // aggregationProcessor.postProcess(searchContext);

        // Post process
        // Create a list to store the InternalValueCount objects
        // Can we map from the preprocess
        List<InternalAggregation> internalAggList = new ArrayList<>();
        Map<String, Object[]> map = searchContext.getDFResults();
        for (Map.Entry<String, Object[]> entry : map.entrySet()) {
            String key = entry.getKey();
            Object[] value = entry.getValue();
            // SUM, Count will work with integer casting, but (Integer) value casting may not work well for avg
            InternalValueCount ivc = new InternalValueCount(key, (long) value[0], null);
            internalAggList.add(ivc);
        }

        final InternalAggregations internalAggregations = InternalAggregations.from(internalAggList);
        QuerySearchResult querySearchResult = searchContext.queryResult();
        querySearchResult.aggregations(internalAggregations);

        if (searchContext.getProfilers() != null) {
            ProfileShardResult shardResults = SearchProfileShardResults.buildShardResults(
                searchContext.getProfilers(),
                searchContext.request()
            );
            searchContext.queryResult().profileResults(shardResults);
        }
    }

    // making public for testing
    public QueryPhaseSearcher getQueryPhaseSearcher() {
        return queryPhaseSearcher;
    }

    /**
     * In a package-private method so that it can be tested without having to
     * wire everything (mapperService, etc.)
     * @return whether the rescoring phase should be executed
     */
    static boolean executeInternal(SearchContext searchContext) throws QueryPhaseExecutionException {
        return executeInternal(searchContext, QueryPhase.DEFAULT_QUERY_PHASE_SEARCHER);
    }

    /**
     * In a package-private method so that it can be tested without having to
     * wire everything (mapperService, etc.)
     * @return whether the rescoring phase should be executed
     */
    static boolean executeInternal(SearchContext searchContext, QueryPhaseSearcher queryPhaseSearcher) throws QueryPhaseExecutionException {
        final ContextIndexSearcher searcher = searchContext.searcher();
        final IndexReader reader = searcher.getIndexReader();
        QuerySearchResult queryResult = searchContext.queryResult();
        queryResult.searchTimedOut(false);
        try {
            queryResult.from(searchContext.from());
            queryResult.size(searchContext.size());
            Query query = searchContext.query();
            assert query == searcher.rewrite(query); // already rewritten

            final ScrollContext scrollContext = searchContext.scrollContext();
            if (scrollContext != null) {
                if (scrollContext.totalHits == null) {
                    // first round
                    assert scrollContext.lastEmittedDoc == null;
                    // there is not much that we can optimize here since we want to collect all
                    // documents in order to get the total number of hits

                } else {
                    final ScoreDoc after = scrollContext.lastEmittedDoc;
                    if (canEarlyTerminate(reader, searchContext.sort())) {
                        // now this gets interesting: since the search sort is a prefix of the index sort, we can directly
                        // skip to the desired doc
                        if (after != null) {
                            query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                                .add(new SearchAfterSortedDocQuery(searchContext.sort().sort, (FieldDoc) after), BooleanClause.Occur.FILTER)
                                .build();
                        }
                    }
                }
            }

            final LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
            // whether the chain contains a collector that filters documents
            boolean hasFilterCollector = false;
            if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER) {
                // add terminate_after before the filter collectors
                // it will only be applied on documents accepted by these filter collectors
                collectors.add(createEarlyTerminationCollectorContext(searchContext.terminateAfter()));
                // this collector can filter documents during the collection
                hasFilterCollector = true;
            }
            if (searchContext.parsedPostFilter() != null) {
                // add post filters before aggregations
                // it will only be applied to top hits
                collectors.add(createFilteredCollectorContext(searcher, searchContext.parsedPostFilter().query()));
                // this collector can filter documents during the collection
                hasFilterCollector = true;
            }

            // plug in additional collectors, like aggregations except global aggregations
            final List<CollectorManager<? extends Collector, ReduceableSearchResult>> managersExceptGlobalAgg = searchContext
                .queryCollectorManagers()
                .entrySet()
                .stream()
                .filter(entry -> !(entry.getKey().equals(GlobalAggCollectorManager.class)))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
            if (managersExceptGlobalAgg.isEmpty() == false) {
                collectors.add(createMultiCollectorContext(managersExceptGlobalAgg));
            }

            if (searchContext.minimumScore() != null) {
                // apply the minimum score after multi collector so we filter aggs as well
                collectors.add(createMinScoreCollectorContext(searchContext.minimumScore()));
                // this collector can filter documents during the collection
                hasFilterCollector = true;
            }

            boolean timeoutSet = scrollContext == null
                && searchContext.timeout() != null
                && searchContext.timeout().equals(SearchService.NO_TIMEOUT) == false;

            final Runnable timeoutRunnable;
            if (timeoutSet) {
                timeoutRunnable = searcher.addQueryCancellation(createQueryTimeoutChecker(searchContext));
            } else {
                timeoutRunnable = null;
            }

            if (searchContext.lowLevelCancellation()) {
                searcher.addQueryCancellation(() -> {
                    SearchShardTask task = searchContext.getTask();
                    if (task != null && task.isCancelled()) {
                        throw new TaskCancelledException("cancelled task with reason: " + task.getReasonCancelled());
                    }
                });
            }

            try {
                boolean shouldRescore = queryPhaseSearcher.searchWith(
                    searchContext,
                    searcher,
                    query,
                    collectors,
                    hasFilterCollector,
                    timeoutSet
                );

                ExecutorService executor = searchContext.indexShard().getThreadPool().executor(ThreadPool.Names.SEARCH);
                if (executor instanceof EWMATrackingThreadPoolExecutor) {
                    final EWMATrackingThreadPoolExecutor rExecutor = (EWMATrackingThreadPoolExecutor) executor;
                    queryResult.nodeQueueSize(rExecutor.getCurrentQueueSize());
                    queryResult.serviceTimeEWMA((long) rExecutor.getTaskExecutionEWMA());
                }

                return shouldRescore;
            } finally {
                // Search phase has finished, no longer need to check for timeout
                // otherwise aggregation phase might get cancelled.
                if (timeoutRunnable != null) {
                    searcher.removeQueryCancellation(timeoutRunnable);
                }
            }
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Failed to execute main query", e);
        }
    }

    /**
     * Create runnable which throws {@link TimeExceededException} when the runnable is called after timeout + runnable creation time
     * exceeds currentTime
     * @param searchContext to extract timeout from and to get relative time from
     * @return the created runnable
     */
    static Runnable createQueryTimeoutChecker(final SearchContext searchContext) {
        /* for startTime, relative non-cached precise time must be used to prevent false positive timeouts.
        * Using cached time for startTime will fail and produce false positive timeouts when maxTime = (startTime + timeout) falls in
        * next time cache slot(s) AND time caching lifespan > passed timeout */
        final long startTime = searchContext.getRelativeTimeInMillis(false);
        final long maxTime = startTime + searchContext.timeout().millis();
        return () -> {
            /* As long as startTime is non cached time, using cached time here might only produce false negative timeouts within the time
            * cache life span which is acceptable */
            final long time = searchContext.getRelativeTimeInMillis();
            if (time > maxTime) {
                throw new TimeExceededException();
            }
        };
    }

    private static boolean searchWithCollector(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        QueryCollectorContext queryCollectorContext,
        boolean hasFilterCollector,
        boolean timeoutSet
    ) throws IOException {
        // add passed collector, the first collector context in the chain
        collectors.addFirst(Objects.requireNonNull(queryCollectorContext));

        final Collector queryCollector;
        if (searchContext.getProfilers() != null) {
            InternalProfileCollector profileCollector = QueryCollectorContext.createQueryCollectorWithProfiler(collectors);
            searchContext.getProfilers().getCurrentQueryProfiler().setCollector(profileCollector);
            queryCollector = profileCollector;
        } else {
            queryCollector = QueryCollectorContext.createQueryCollector(collectors);
        }
        QuerySearchResult queryResult = searchContext.queryResult();
        try {
            searcher.search(query, queryCollector);
        } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
            // EarlyTerminationException is not caught in ContextIndexSearcher to allow force termination of collection. Postcollection
            // still needs to be processed for Aggregations when early termination takes place.
            searchContext.bucketCollectorProcessor().processPostCollection(queryCollector);
            queryResult.terminatedEarly(true);
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
        for (QueryCollectorContext ctx : collectors) {
            ctx.postProcess(queryResult);
        }
        if (queryCollectorContext instanceof RescoringQueryCollectorContext) {
            return ((RescoringQueryCollectorContext) queryCollectorContext).shouldRescore();
        }
        return false;
    }

    /**
     * Returns whether collection within the provided <code>reader</code> can be early-terminated if it sorts
     * with <code>sortAndFormats</code>.
     **/
    private static boolean canEarlyTerminate(IndexReader reader, SortAndFormats sortAndFormats) {
        if (sortAndFormats == null || sortAndFormats.sort == null) {
            return false;
        }
        final Sort sort = sortAndFormats.sort;
        for (LeafReaderContext ctx : reader.leaves()) {
            Sort indexSort = ctx.reader().getMetaData().sort();
            if (indexSort == null || Lucene.canEarlyTerminate(sort, indexSort) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * The exception being raised when search timeout is reached.
     *
     * @opensearch.internal
     */
    public static class TimeExceededException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Default {@link QueryPhaseSearcher} implementation which delegates to the {@link QueryPhase}.
     *
     * @opensearch.internal
     */
    public static class DefaultQueryPhaseSearcher implements QueryPhaseSearcher {
        private final AggregationProcessor aggregationProcessor;

        /**
         * Please use {@link QueryPhase#DEFAULT_QUERY_PHASE_SEARCHER}
         */
        protected DefaultQueryPhaseSearcher() {
            aggregationProcessor = new DefaultAggregationProcessor();
        }

        @Override
        public boolean searchWith(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) throws IOException {
            return searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }

        @Override
        public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
            return aggregationProcessor;
        }

        protected boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) throws IOException {
            QueryCollectorContext queryCollectorContext = getQueryCollectorContext(searchContext, query, hasFilterCollector);
            return searchWithCollector(searchContext, searcher, query, collectors, queryCollectorContext, hasFilterCollector, hasTimeout);
        }

        private QueryCollectorContext getQueryCollectorContext(SearchContext searchContext, Query query, boolean hasFilterCollector)
            throws IOException {
            // create the top docs collector last when the other collectors are known
            final Optional<QueryCollectorContext> queryCollectorContextOpt = QueryCollectorContextSpecRegistry.getQueryCollectorContextSpec(
                searchContext,
                query,
                new QueryCollectorArguments.Builder().hasFilterCollector(hasFilterCollector).build()
            ).map(queryCollectorContextSpec -> new QueryCollectorContext(queryCollectorContextSpec.getContextName()) {
                @Override
                Collector create(Collector in) throws IOException {
                    return queryCollectorContextSpec.create(in);
                }

                @Override
                CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in)
                    throws IOException {
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

        protected boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            QueryCollectorContext queryCollectorContext,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) throws IOException {
            return QueryPhase.searchWithCollector(
                searchContext,
                searcher,
                query,
                collectors,
                queryCollectorContext,
                hasFilterCollector,
                hasTimeout
            );
        }
    }
}
