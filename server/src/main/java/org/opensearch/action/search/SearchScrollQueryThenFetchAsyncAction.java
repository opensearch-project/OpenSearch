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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.ShardFetchRequest;
import org.opensearch.search.internal.InternalScrollSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ScrollQuerySearchResult;
import org.opensearch.transport.Transport;

import java.util.function.BiFunction;

/**
 * async action for a search scroll query then fetch
 *
 * @opensearch.internal
 */
final class SearchScrollQueryThenFetchAsyncAction extends SearchScrollAsyncAction<ScrollQuerySearchResult> {

    private final SearchTask task;
    private final AtomicArray<FetchSearchResult> fetchResults;
    private final AtomicArray<QuerySearchResult> queryResults;

    SearchScrollQueryThenFetchAsyncAction(
        Logger logger,
        ClusterService clusterService,
        SearchTransportService searchTransportService,
        SearchPhaseController searchPhaseController,
        SearchScrollRequest request,
        SearchTask task,
        ParsedScrollId scrollId,
        ActionListener<SearchResponse> listener
    ) {
        super(scrollId, logger, clusterService.state().nodes(), listener, searchPhaseController, request, searchTransportService);
        this.task = task;
        this.fetchResults = new AtomicArray<>(scrollId.getContext().length);
        this.queryResults = new AtomicArray<>(scrollId.getContext().length);
    }

    protected void onFirstPhaseResult(int shardId, ScrollQuerySearchResult result) {
        queryResults.setOnce(shardId, result.queryResult());
    }

    @Override
    protected void executeInitialPhase(
        Transport.Connection connection,
        InternalScrollSearchRequest internalRequest,
        SearchActionListener<ScrollQuerySearchResult> searchActionListener
    ) {
        searchTransportService.sendExecuteScrollQuery(connection, internalRequest, task, searchActionListener);
    }

    @Override
    protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
        return new SearchPhase("fetch") {
            @Override
            public void run() {
                final SearchPhaseController.ReducedQueryPhase reducedQueryPhase = searchPhaseController.reducedScrollQueryPhase(
                    queryResults.asList()
                );
                ScoreDoc[] scoreDocs = reducedQueryPhase.sortedTopDocs.scoreDocs;
                if (scoreDocs.length == 0) {
                    sendResponse(reducedQueryPhase, fetchResults);
                    return;
                }

                final IntArrayList[] docIdsToLoad = searchPhaseController.fillDocIdsToLoad(queryResults.length(), scoreDocs);
                final ScoreDoc[] lastEmittedDocPerShard = searchPhaseController.getLastEmittedDocPerShard(
                    reducedQueryPhase,
                    queryResults.length()
                );
                final CountDown counter = new CountDown(docIdsToLoad.length);
                for (int i = 0; i < docIdsToLoad.length; i++) {
                    final int index = i;
                    final IntArrayList docIds = docIdsToLoad[index];
                    if (docIds != null) {
                        final QuerySearchResult querySearchResult = queryResults.get(index);
                        ScoreDoc lastEmittedDoc = lastEmittedDocPerShard[index];
                        ShardFetchRequest shardFetchRequest = new ShardFetchRequest(
                            querySearchResult.getContextId(),
                            docIds,
                            lastEmittedDoc
                        );
                        SearchShardTarget searchShardTarget = querySearchResult.getSearchShardTarget();
                        DiscoveryNode node = clusterNodeLookup.apply(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
                        assert node != null : "target node is null in secondary phase";
                        Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), node);
                        searchTransportService.sendExecuteFetchScroll(
                            connection,
                            shardFetchRequest,
                            task,
                            new SearchActionListener<FetchSearchResult>(querySearchResult.getSearchShardTarget(), index) {
                                @Override
                                protected void innerOnResponse(FetchSearchResult response) {
                                    fetchResults.setOnce(response.getShardIndex(), response);
                                    if (counter.countDown()) {
                                        sendResponse(reducedQueryPhase, fetchResults);
                                    }
                                }

                                @Override
                                public void onFailure(Exception t) {
                                    onShardFailure(
                                        getName(),
                                        counter,
                                        querySearchResult.getContextId(),
                                        t,
                                        querySearchResult.getSearchShardTarget(),
                                        () -> sendResponsePhase(reducedQueryPhase, fetchResults)
                                    );
                                }
                            }
                        );
                    } else {
                        // the counter is set to the total size of docIdsToLoad
                        // which can have null values so we have to count them down too
                        if (counter.countDown()) {
                            sendResponse(reducedQueryPhase, fetchResults);
                        }
                    }
                }
            }
        };
    }

}
