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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.InternalScrollSearchRequest;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.Transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Abstract base class for scroll execution modes. This class encapsulates the basic logic to
 * fan out to nodes and execute the query part of the scroll request. Subclasses can for instance
 * run separate fetch phases etc.
 *
 * @opensearch.internal
 */
abstract class SearchScrollAsyncAction<T extends SearchPhaseResult> implements Runnable {
    protected final Logger logger;
    protected final ActionListener<SearchResponse> listener;
    protected final ParsedScrollId scrollId;
    protected final DiscoveryNodes nodes;
    protected final SearchPhaseController searchPhaseController;
    protected final SearchScrollRequest request;
    protected final SearchTransportService searchTransportService;
    private final long startTime;
    private final List<ShardSearchFailure> shardFailures = new ArrayList<>();
    private final AtomicInteger successfulOps;

    protected SearchScrollAsyncAction(
        ParsedScrollId scrollId,
        Logger logger,
        DiscoveryNodes nodes,
        ActionListener<SearchResponse> listener,
        SearchPhaseController searchPhaseController,
        SearchScrollRequest request,
        SearchTransportService searchTransportService
    ) {
        this.startTime = System.currentTimeMillis();
        this.scrollId = scrollId;
        this.successfulOps = new AtomicInteger(scrollId.getContext().length);
        this.logger = logger;
        this.listener = listener;
        this.nodes = nodes;
        this.searchPhaseController = searchPhaseController;
        this.request = request;
        this.searchTransportService = searchTransportService;
    }

    /**
     * Builds how long it took to execute the search.
     */
    private long buildTookInMillis() {
        // protect ourselves against time going backwards
        // negative values don't make sense and we want to be able to serialize that thing as a vLong
        return Math.max(1, System.currentTimeMillis() - startTime);
    }

    public final void run() {
        final SearchContextIdForNode[] context = scrollId.getContext();
        if (context.length == 0) {
            listener.onFailure(new SearchPhaseExecutionException("query", "no nodes to search on", ShardSearchFailure.EMPTY_ARRAY));
        } else {
            collectNodesAndRun(
                Arrays.asList(context),
                nodes,
                searchTransportService,
                ActionListener.wrap(lookup -> run(lookup, context), listener::onFailure)
            );
        }
    }

    /**
     * This method collects nodes from the remote clusters asynchronously if any of the scroll IDs references a remote cluster.
     * Otherwise the action listener will be invoked immediately with a function based on the given discovery nodes.
     */
    static void collectNodesAndRun(
        final Iterable<SearchContextIdForNode> scrollIds,
        DiscoveryNodes nodes,
        SearchTransportService searchTransportService,
        ActionListener<BiFunction<String, String, DiscoveryNode>> listener
    ) {
        Set<String> clusters = new HashSet<>();
        for (SearchContextIdForNode target : scrollIds) {
            if (target.getClusterAlias() != null) {
                clusters.add(target.getClusterAlias());
            }
        }
        if (clusters.isEmpty()) { // no remote clusters
            listener.onResponse((cluster, node) -> nodes.get(node));
        } else {
            RemoteClusterService remoteClusterService = searchTransportService.getRemoteClusterService();
            remoteClusterService.collectNodes(
                clusters,
                ActionListener.map(
                    listener,
                    nodeFunction -> (clusterAlias, node) -> clusterAlias == null ? nodes.get(node) : nodeFunction.apply(clusterAlias, node)
                )
            );
        }
    }

    private void run(BiFunction<String, String, DiscoveryNode> clusterNodeLookup, final SearchContextIdForNode[] context) {
        final CountDown counter = new CountDown(scrollId.getContext().length);
        for (int i = 0; i < context.length; i++) {
            SearchContextIdForNode target = context[i];
            final int shardIndex = i;
            final Transport.Connection connection;
            try {
                DiscoveryNode node = clusterNodeLookup.apply(target.getClusterAlias(), target.getNode());
                if (node == null) {
                    throw new IllegalStateException("node [" + target.getNode() + "] is not available");
                }
                connection = getConnection(target.getClusterAlias(), node);
            } catch (Exception ex) {
                onShardFailure(
                    "query",
                    counter,
                    target.getSearchContextId(),
                    ex,
                    null,
                    () -> SearchScrollAsyncAction.this.moveToNextPhase(clusterNodeLookup)
                );
                continue;
            }
            final InternalScrollSearchRequest internalRequest = TransportSearchHelper.internalScrollSearchRequest(
                target.getSearchContextId(),
                request
            );
            // we can't create a SearchShardTarget here since we don't know the index and shard ID we are talking to
            // we only know the node and the search context ID. Yet, the response will contain the SearchShardTarget
            // from the target node instead...that's why we pass null here
            SearchActionListener<T> searchActionListener = new SearchActionListener<T>(null, shardIndex) {

                @Override
                protected void setSearchShardTarget(T response) {
                    // don't do this - it's part of the response...
                    assert response.getSearchShardTarget() != null : "search shard target must not be null";
                    if (target.getClusterAlias() != null) {
                        // re-create the search target and add the cluster alias if there is any,
                        // we need this down the road for subseq. phases
                        SearchShardTarget searchShardTarget = response.getSearchShardTarget();
                        response.setSearchShardTarget(
                            new SearchShardTarget(
                                searchShardTarget.getNodeId(),
                                searchShardTarget.getShardId(),
                                target.getClusterAlias(),
                                null
                            )
                        );
                    }
                }

                @Override
                protected void innerOnResponse(T result) {
                    assert shardIndex == result.getShardIndex() : "shard index mismatch: "
                        + shardIndex
                        + " but got: "
                        + result.getShardIndex();
                    onFirstPhaseResult(shardIndex, result);
                    if (counter.countDown()) {
                        SearchPhase phase = moveToNextPhase(clusterNodeLookup);
                        try {
                            phase.run();
                        } catch (Exception e) {
                            // we need to fail the entire request here - the entire phase just blew up
                            // don't call onShardFailure or onFailure here since otherwise we'd countDown the counter
                            // again which would result in an exception
                            listener.onFailure(
                                new SearchPhaseExecutionException(phase.getName(), "Phase failed", e, ShardSearchFailure.EMPTY_ARRAY)
                            );
                        }
                    }
                }

                @Override
                public void onFailure(Exception t) {
                    onShardFailure(
                        "query",
                        counter,
                        target.getSearchContextId(),
                        t,
                        null,
                        () -> SearchScrollAsyncAction.this.moveToNextPhase(clusterNodeLookup)
                    );
                }
            };
            executeInitialPhase(connection, internalRequest, searchActionListener);
        }
    }

    synchronized ShardSearchFailure[] buildShardFailures() { // pkg private for testing
        if (shardFailures.isEmpty()) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        return shardFailures.toArray(new ShardSearchFailure[0]);
    }

    // we do our best to return the shard failures, but its ok if its not fully concurrently safe
    // we simply try and return as much as possible
    private synchronized void addShardFailure(ShardSearchFailure failure) {
        shardFailures.add(failure);
    }

    protected abstract void executeInitialPhase(
        Transport.Connection connection,
        InternalScrollSearchRequest internalRequest,
        SearchActionListener<T> searchActionListener
    );

    protected abstract SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup);

    protected abstract void onFirstPhaseResult(int shardId, T result);

    protected SearchPhase sendResponsePhase(
        SearchPhaseController.ReducedQueryPhase queryPhase,
        final AtomicArray<? extends SearchPhaseResult> fetchResults
    ) {
        return new SearchPhase(SearchPhaseName.FETCH.getName()) {
            @Override
            public void run() throws IOException {
                sendResponse(queryPhase, fetchResults);
            }
        };
    }

    protected final void sendResponse(
        SearchPhaseController.ReducedQueryPhase queryPhase,
        final AtomicArray<? extends SearchPhaseResult> fetchResults
    ) {
        try {
            final InternalSearchResponse internalResponse = searchPhaseController.merge(
                true,
                queryPhase,
                fetchResults.asList(),
                fetchResults::get
            );
            // the scroll ID never changes we always return the same ID. This ID contains all the shards and their context ids
            // such that we can talk to them again in the next roundtrip.
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = request.scrollId();
            }
            listener.onResponse(
                new SearchResponse(
                    internalResponse,
                    scrollId,
                    this.scrollId.getContext().length,
                    successfulOps.get(),
                    0,
                    buildTookInMillis(),
                    buildShardFailures(),
                    SearchResponse.Clusters.EMPTY,
                    null
                )
            );
        } catch (Exception e) {
            listener.onFailure(new ReduceSearchPhaseException("fetch", "inner finish failed", e, buildShardFailures()));
        }
    }

    protected void onShardFailure(
        String phaseName,
        final CountDown counter,
        final ShardSearchContextId searchId,
        Exception failure,
        @Nullable SearchShardTarget searchShardTarget,
        Supplier<SearchPhase> nextPhaseSupplier
    ) {
        if (logger.isDebugEnabled()) {
            logger.debug(new ParameterizedMessage("[{}] Failed to execute {} phase", searchId, phaseName), failure);
        }
        addShardFailure(new ShardSearchFailure(failure, searchShardTarget));
        int successfulOperations = successfulOps.decrementAndGet();
        assert successfulOperations >= 0 : "successfulOperations must be >= 0 but was: " + successfulOperations;
        if (counter.countDown()) {
            if (successfulOps.get() == 0) {
                listener.onFailure(new SearchPhaseExecutionException(phaseName, "all shards failed", failure, buildShardFailures()));
            } else {
                SearchPhase phase = nextPhaseSupplier.get();
                try {
                    phase.run();
                } catch (Exception e) {
                    e.addSuppressed(failure);
                    listener.onFailure(
                        new SearchPhaseExecutionException(phase.getName(), "Phase failed", e, ShardSearchFailure.EMPTY_ARRAY)
                    );
                }
            }
        }
    }

    protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
        return searchTransportService.getConnection(clusterAlias, node);
    }
}
