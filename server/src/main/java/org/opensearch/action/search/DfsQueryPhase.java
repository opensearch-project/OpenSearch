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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.dfs.AggregatedDfs;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.query.QuerySearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.transport.Transport;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * This search phase fans out to every shards to execute a distributed search with a pre-collected distributed frequencies for all
 * search terms used in the actual search query. This phase is very similar to a the default query-then-fetch search phase but it doesn't
 * retry on another shard if any of the shards are failing. Failures are treated as shard failures and are counted as a non-successful
 * operation.
 * @see CountedCollector#onFailure(int, SearchShardTarget, Exception)
 *
 * @opensearch.internal
 */
final class DfsQueryPhase extends SearchPhase {
    private final ArraySearchPhaseResults<SearchPhaseResult> queryResult;
    private final List<DfsSearchResult> searchResults;
    private final AggregatedDfs dfs;
    private final Function<ArraySearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final SearchTransportService searchTransportService;
    private final SearchProgressListener progressListener;

    DfsQueryPhase(
        List<DfsSearchResult> searchResults,
        AggregatedDfs dfs,
        QueryPhaseResultConsumer queryResult,
        Function<ArraySearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory,
        SearchPhaseContext context
    ) {
        super("dfs_query");
        this.progressListener = context.getTask().getProgressListener();
        this.queryResult = queryResult;
        this.searchResults = searchResults;
        this.dfs = dfs;
        this.nextPhaseFactory = nextPhaseFactory;
        this.context = context;
        this.searchTransportService = context.getSearchTransport();

        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        context.addReleasable(queryResult);
    }

    @Override
    public void run() throws IOException {
        // TODO we can potentially also consume the actual per shard results from the initial phase here in the aggregateDfs
        // to free up memory early
        final CountedCollector<SearchPhaseResult> counter = new CountedCollector<>(
            queryResult,
            searchResults.size(),
            () -> context.executeNextPhase(this, nextPhaseFactory.apply(queryResult)),
            context
        );
        for (final DfsSearchResult dfsResult : searchResults) {
            final SearchShardTarget searchShardTarget = dfsResult.getSearchShardTarget();
            Transport.Connection connection = context.getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(
                searchShardTarget.getOriginalIndices(),
                dfsResult.getContextId(),
                dfsResult.getShardSearchRequest(),
                dfs
            );
            final int shardIndex = dfsResult.getShardIndex();
            searchTransportService.sendExecuteQuery(
                connection,
                querySearchRequest,
                context.getTask(),
                new SearchActionListener<QuerySearchResult>(searchShardTarget, shardIndex) {

                    @Override
                    protected void innerOnResponse(QuerySearchResult response) {
                        try {
                            counter.onResult(response);
                        } catch (Exception e) {
                            context.onPhaseFailure(DfsQueryPhase.this, "", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        try {
                            context.getLogger()
                                .debug(
                                    () -> new ParameterizedMessage("[{}] Failed to execute query phase", querySearchRequest.contextId()),
                                    exception
                                );
                            progressListener.notifyQueryFailure(shardIndex, searchShardTarget, exception);
                            counter.onFailure(shardIndex, searchShardTarget, exception);
                        } finally {
                            if (context.getRequest().pointInTimeBuilder() == null) {
                                // the query might not have been executed at all (for example because thread pool rejected
                                // execution) and the search context that was created in dfs phase might not be released.
                                // release it again to be in the safe side
                                context.sendReleaseSearchContext(
                                    querySearchRequest.contextId(),
                                    connection,
                                    searchShardTarget.getOriginalIndices()
                                );
                            }
                        }
                    }
                }
            );
        }
    }
}
