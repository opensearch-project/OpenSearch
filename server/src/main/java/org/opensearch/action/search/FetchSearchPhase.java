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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.search.ScoreDoc;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.search.RescoreDocIds;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.dfs.AggregatedDfs;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.ShardFetchSearchRequest;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.transport.Transport;

import java.util.List;
import java.util.function.BiFunction;

/**
 * This search phase merges the query results from the previous phase together and calculates the topN hits for this search.
 * Then it reaches out to all relevant shards to fetch the topN hits.
 *
 * @opensearch.internal
 */
final class FetchSearchPhase extends SearchPhase {
    private final ArraySearchPhaseResults<FetchSearchResult> fetchResults;
    private final SearchPhaseController searchPhaseController;
    private final AtomicArray<SearchPhaseResult> queryResults;
    private final BiFunction<InternalSearchResponse, AtomicArray<SearchPhaseResult>, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final Logger logger;
    private final SearchPhaseResults<SearchPhaseResult> resultConsumer;
    private final SearchProgressListener progressListener;
    private final AggregatedDfs aggregatedDfs;

    FetchSearchPhase(
        SearchPhaseResults<SearchPhaseResult> resultConsumer,
        SearchPhaseController searchPhaseController,
        AggregatedDfs aggregatedDfs,
        SearchPhaseContext context
    ) {
        this(
            resultConsumer,
            searchPhaseController,
            aggregatedDfs,
            context,
            (response, queryPhaseResults) -> new ExpandSearchPhase(context, response, queryPhaseResults)
        );
    }

    FetchSearchPhase(
        SearchPhaseResults<SearchPhaseResult> resultConsumer,
        SearchPhaseController searchPhaseController,
        AggregatedDfs aggregatedDfs,
        SearchPhaseContext context,
        BiFunction<InternalSearchResponse, AtomicArray<SearchPhaseResult>, SearchPhase> nextPhaseFactory
    ) {
        super(SearchPhaseName.FETCH.getName());
        if (context.getNumShards() != resultConsumer.getNumShards()) {
            throw new IllegalStateException(
                "number of shards must match the length of the query results but doesn't:"
                    + context.getNumShards()
                    + "!="
                    + resultConsumer.getNumShards()
            );
        }
        this.fetchResults = new ArraySearchPhaseResults<>(resultConsumer.getNumShards());
        this.searchPhaseController = searchPhaseController;
        this.queryResults = resultConsumer.getAtomicArray();
        this.aggregatedDfs = aggregatedDfs;
        this.nextPhaseFactory = nextPhaseFactory;
        this.context = context;
        this.logger = context.getLogger();
        this.resultConsumer = resultConsumer;
        this.progressListener = context.getTask().getProgressListener();
    }

    @Override
    public void run() {
        context.execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                // we do the heavy lifting in this inner run method where we reduce aggs etc. that's why we fork this phase
                // off immediately instead of forking when we send back the response to the user since there we only need
                // to merge together the fetched results which is a linear operation.
                innerRun();
            }

            @Override
            public void onFailure(Exception e) {
                context.onPhaseFailure(FetchSearchPhase.this, "", e);
            }
        });
    }

    private void innerRun() throws Exception {
        final int numShards = context.getNumShards();
        final boolean isScrollSearch = context.getRequest().scroll() != null;
        final List<SearchPhaseResult> phaseResults = queryResults.asList();
        final SearchPhaseController.ReducedQueryPhase reducedQueryPhase = resultConsumer.reduce();
        final boolean queryAndFetchOptimization = queryResults.length() == 1;
        final Runnable finishPhase = () -> moveToNextPhase(
            searchPhaseController,
            queryResults,
            reducedQueryPhase,
            queryAndFetchOptimization ? queryResults : fetchResults.getAtomicArray()
        );
        if (queryAndFetchOptimization) {
            assert phaseResults.isEmpty() || phaseResults.get(0).fetchResult() != null : "phaseResults empty ["
                + phaseResults.isEmpty()
                + "], single result: "
                + phaseResults.get(0).fetchResult();
            // query AND fetch optimization
            finishPhase.run();
        } else {
            ScoreDoc[] scoreDocs = reducedQueryPhase.sortedTopDocs.scoreDocs;
            final List<Integer>[] docIdsToLoad = searchPhaseController.fillDocIdsToLoad(numShards, scoreDocs);
            // no docs to fetch -- sidestep everything and return
            if (scoreDocs.length == 0) {
                // we have to release contexts here to free up resources
                phaseResults.stream().map(SearchPhaseResult::queryResult).forEach(this::releaseIrrelevantSearchContext);
                finishPhase.run();
            } else {
                final ScoreDoc[] lastEmittedDocPerShard = isScrollSearch
                    ? searchPhaseController.getLastEmittedDocPerShard(reducedQueryPhase, numShards)
                    : null;
                final CountedCollector<FetchSearchResult> counter = new CountedCollector<>(
                    fetchResults,
                    docIdsToLoad.length, // we count down every shard in the result no matter if we got any results or not
                    finishPhase,
                    context
                );
                for (int i = 0; i < docIdsToLoad.length; i++) {
                    List<Integer> entry = docIdsToLoad[i];
                    SearchPhaseResult queryResult = queryResults.get(i);
                    if (entry == null) { // no results for this shard ID
                        if (queryResult != null) {
                            // if we got some hits from this shard we have to release the context there
                            // we do this as we go since it will free up resources and passing on the request on the
                            // transport layer is cheap.
                            releaseIrrelevantSearchContext(queryResult.queryResult());
                            progressListener.notifyFetchResult(i);
                        }
                        // in any case we count down this result since we don't talk to this shard anymore
                        counter.countDown();
                    } else {
                        SearchShardTarget searchShardTarget = queryResult.getSearchShardTarget();
                        Transport.Connection connection = context.getConnection(
                            searchShardTarget.getClusterAlias(),
                            searchShardTarget.getNodeId()
                        );
                        ShardFetchSearchRequest fetchSearchRequest = createFetchRequest(
                            queryResult.queryResult().getContextId(),
                            i,
                            entry,
                            lastEmittedDocPerShard,
                            searchShardTarget.getOriginalIndices(),
                            queryResult.getShardSearchRequest(),
                            queryResult.getRescoreDocIds()
                        );
                        executeFetch(i, searchShardTarget, counter, fetchSearchRequest, queryResult.queryResult(), connection);
                    }
                }
            }
        }
    }

    protected ShardFetchSearchRequest createFetchRequest(
        ShardSearchContextId contextId,
        int index,
        List<Integer> entry,
        ScoreDoc[] lastEmittedDocPerShard,
        OriginalIndices originalIndices,
        ShardSearchRequest shardSearchRequest,
        RescoreDocIds rescoreDocIds
    ) {
        final ScoreDoc lastEmittedDoc = (lastEmittedDocPerShard != null) ? lastEmittedDocPerShard[index] : null;
        return new ShardFetchSearchRequest(
            originalIndices,
            contextId,
            shardSearchRequest,
            entry,
            lastEmittedDoc,
            rescoreDocIds,
            aggregatedDfs
        );
    }

    private void executeFetch(
        final int shardIndex,
        final SearchShardTarget shardTarget,
        final CountedCollector<FetchSearchResult> counter,
        final ShardFetchSearchRequest fetchSearchRequest,
        final QuerySearchResult querySearchResult,
        final Transport.Connection connection
    ) {
        context.getSearchTransport()
            .sendExecuteFetch(
                connection,
                fetchSearchRequest,
                context.getTask(),
                new SearchActionListener<FetchSearchResult>(shardTarget, shardIndex) {
                    @Override
                    public void innerOnResponse(FetchSearchResult result) {
                        try {
                            progressListener.notifyFetchResult(shardIndex);
                            context.setPhaseResourceUsages();
                            counter.onResult(result);
                        } catch (Exception e) {
                            context.onPhaseFailure(FetchSearchPhase.this, "", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            logger.debug(
                                () -> new ParameterizedMessage("[{}] Failed to execute fetch phase", fetchSearchRequest.contextId()),
                                e
                            );
                            progressListener.notifyFetchFailure(shardIndex, shardTarget, e);
                            context.setPhaseResourceUsages();
                            counter.onFailure(shardIndex, shardTarget, e);
                        } finally {
                            // the search context might not be cleared on the node where the fetch was executed for example
                            // because the action was rejected by the thread pool. in this case we need to send a dedicated
                            // request to clear the search context.
                            releaseIrrelevantSearchContext(querySearchResult);
                        }
                    }
                }
            );
    }

    /**
     * Releases shard targets that are not used in the docsIdsToLoad.
     */
    private void releaseIrrelevantSearchContext(QuerySearchResult queryResult) {
        // we only release search context that we did not fetch from, if we are not scrolling
        // or using a PIT and if it has at least one hit that didn't make it to the global topDocs
        if (queryResult.hasSearchContext() && context.getRequest().scroll() == null && context.getRequest().pointInTimeBuilder() == null) {
            try {
                SearchShardTarget searchShardTarget = queryResult.getSearchShardTarget();
                Transport.Connection connection = context.getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
                context.sendReleaseSearchContext(queryResult.getContextId(), connection, searchShardTarget.getOriginalIndices());
            } catch (Exception e) {
                context.getLogger().trace("failed to release context", e);
            }
        }
    }

    private void moveToNextPhase(
        SearchPhaseController searchPhaseController,
        AtomicArray<SearchPhaseResult> queryPhaseResults,
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase,
        AtomicArray<? extends SearchPhaseResult> fetchResultsArr
    ) {
        final InternalSearchResponse internalResponse = searchPhaseController.merge(
            context.getRequest().scroll() != null,
            reducedQueryPhase,
            fetchResultsArr.asList(),
            fetchResultsArr::get
        );
        context.executeNextPhase(this, nextPhaseFactory.apply(internalResponse, queryPhaseResults));
    }
}
