/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Data-node side handler for {@link NodeSearchRequest}. Iterates the request's per-shard
 * sub-requests, hands each one to {@link SearchService#executeQueryPhase(ShardSearchRequest, boolean, SearchShardTask, ActionListener)}
 * and aggregates the per-shard results back into a single {@link NodeSearchResponse}.
 *
 * @opensearch.internal
 */
@InternalApi
public class TransportNodeSearchAction {

    private final SearchService searchService;

    public TransportNodeSearchAction(SearchService searchService) {
        this.searchService = searchService;
    }

    void messageReceived(NodeSearchRequest request, TransportChannel channel, Task parentTask) {
        new QueryThenFetchSearchAction(searchService, request, channel, parentTask).start();
    }

    void messageReceivedCanMatch(NodeSearchRequest request, TransportChannel channel, Task parentTask) {
        new CanMatchSearchAction(searchService, request, channel, parentTask).start();
    }

    private abstract static class NodeSearchAction<Result> {

        final SearchService searchService;
        final NodeSearchRequest request;
        final SearchShardTask task;

        private final Map<Integer, Result> results = new ConcurrentHashMap<>();
        private final Map<Integer, Exception> failures = new ConcurrentHashMap<>();
        private final AtomicInteger pending;

        NodeSearchAction(SearchService searchService, NodeSearchRequest request, Task parentTask) {
            this.searchService = searchService;
            this.request = request;
            this.task = (parentTask instanceof SearchShardTask) ? (SearchShardTask) parentTask : null;
            this.pending = new AtomicInteger(request.shardCount());
        }

        void start() {
            if (request.shardCount() == 0) {
                sendResponse(new ArrayList<>(), new ArrayList<>());
                return;
            }
            for (int i = 0; i < request.shardCount(); i++) {
                performShard(i);
            }
        }

        private void performShard(int shardIndex) {
            if (task != null && task.isCancelled()) {
                complete(shardIndex, null, new TaskCancelledException("parent search task was cancelled: " + task.getReasonCancelled()));
                return;
            }
            final ShardSearchRequest shardRequest;
            try {
                shardRequest = request.shardRequest(shardIndex);
            } catch (Exception e) {
                complete(shardIndex, null, e);
                return;
            }
            executeShard(shardRequest, new ActionListener<>() {
                @Override
                public void onResponse(Result result) {
                    complete(shardIndex, result, null);
                    // if we already received a search result we can inform the shard that it
                    // can return a null response if the request rewrites to match none rather
                    // than creating an empty response in the search thread pool.
                    // Note that, we have to disable this shortcut for queries that create a context (scroll and search context).
                    request.canReturnNullResponseIfMatchNoDocs(shardRequest.scroll() == null);
                }

                @Override
                public void onFailure(Exception e) {
                    complete(shardIndex, null, e);
                }
            });
        }

        abstract void executeShard(ShardSearchRequest shardRequest, ActionListener<Result> listener);

        abstract void sendResponse(List<Result> orderedResults, List<Exception> orderedFailures);

        private void complete(int shardIndex, Result result, Exception failure) {
            assert (result == null) != (failure == null) : "shard response must contain either a result or a failure";
            if (result != null) {
                results.putIfAbsent(shardIndex, result);
            } else {
                failures.putIfAbsent(shardIndex, failure);
            }
            if (pending.decrementAndGet() != 0) {
                return;
            }
            final List<Result> orderedResults = new ArrayList<>(request.shardCount());
            final List<Exception> orderedFailures = new ArrayList<>(request.shardCount());
            for (int i = 0; i < request.shardCount(); i++) {
                orderedResults.add(results.get(i));
                orderedFailures.add(failures.get(i));
            }
            sendResponse(orderedResults, orderedFailures);
        }
    }

    /**
     * Holds the mutable per-request state for handling a single {@link NodeSearchRequest} so that the
     * shard dispatch and the per-shard callbacks can share it without threading a context object through
     * every call.
     */
    private static final class QueryThenFetchSearchAction extends NodeSearchAction<SearchPhaseResult> {

        private final ChannelActionListener<NodeSearchResponse<SearchPhaseResult>, NodeSearchRequest> channelListener;

        QueryThenFetchSearchAction(SearchService searchService, NodeSearchRequest request, TransportChannel channel, Task parentTask) {
            super(searchService, request, parentTask);
            this.channelListener = new ChannelActionListener<>(channel, SearchTransportService.QUERY_NODE_ACTION_NAME, request);
        }

        @Override
        void executeShard(ShardSearchRequest shardRequest, ActionListener<SearchPhaseResult> listener) {
            searchService.executeQueryPhase(shardRequest, false, task, listener, null, false);
        }

        @Override
        void sendResponse(List<SearchPhaseResult> orderedResults, List<Exception> orderedFailures) {
            channelListener.onResponse(new NodeSearchResponse<>(orderedResults, orderedFailures));
        }
    }

    private static final class CanMatchSearchAction extends NodeSearchAction<SearchService.CanMatchResponse> {

        private final ChannelActionListener<NodeSearchResponse<SearchService.CanMatchResponse>, NodeSearchRequest> channelListener;

        CanMatchSearchAction(SearchService searchService, NodeSearchRequest request, TransportChannel channel, Task parentTask) {
            super(searchService, request, parentTask);
            this.channelListener = new ChannelActionListener<>(channel, SearchTransportService.QUERY_CAN_MATCH_NODE_NAME, request);
        }

        @Override
        void executeShard(ShardSearchRequest shardRequest, ActionListener<SearchService.CanMatchResponse> listener) {
            searchService.canMatch(shardRequest, listener);
        }

        @Override
        void sendResponse(List<SearchService.CanMatchResponse> orderedResults, List<Exception> orderedFailures) {
            channelListener.onResponse(new NodeSearchResponse<>(orderedResults, orderedFailures));
        }
    }
}
