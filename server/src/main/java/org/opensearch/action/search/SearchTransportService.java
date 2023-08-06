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

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.QueryFetchSearchResult;
import org.opensearch.search.fetch.ScrollQueryFetchSearchResult;
import org.opensearch.search.fetch.ShardFetchRequest;
import org.opensearch.search.fetch.ShardFetchSearchRequest;
import org.opensearch.search.internal.InternalScrollSearchRequest;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ScrollQuerySearchResult;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportActionProxy;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * An encapsulation of {@link org.opensearch.search.SearchService} operations exposed through
 * transport.
 *
 * @opensearch.internal
 */
public class SearchTransportService {

    public static final String FREE_CONTEXT_SCROLL_ACTION_NAME = "indices:data/read/search[free_context/scroll]";
    public static final String FREE_CONTEXT_ACTION_NAME = "indices:data/read/search[free_context]";
    public static final String CLEAR_SCROLL_CONTEXTS_ACTION_NAME = "indices:data/read/search[clear_scroll_contexts]";
    public static final String FREE_PIT_CONTEXT_ACTION_NAME = "indices:data/read/search[free_context/pit]";
    public static final String FREE_ALL_PIT_CONTEXTS_ACTION_NAME = "indices:data/read/search[free_pit_contexts]";
    public static final String DFS_ACTION_NAME = "indices:data/read/search[phase/dfs]";
    public static final String QUERY_ACTION_NAME = "indices:data/read/search[phase/query]";
    public static final String QUERY_ID_ACTION_NAME = "indices:data/read/search[phase/query/id]";
    public static final String QUERY_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query/scroll]";
    public static final String QUERY_FETCH_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query+fetch/scroll]";
    public static final String FETCH_ID_SCROLL_ACTION_NAME = "indices:data/read/search[phase/fetch/id/scroll]";
    public static final String FETCH_ID_ACTION_NAME = "indices:data/read/search[phase/fetch/id]";
    public static final String QUERY_CAN_MATCH_NAME = "indices:data/read/search[can_match]";
    public static final String CREATE_READER_CONTEXT_ACTION_NAME = "indices:data/read/search[create_context]";
    public static final String UPDATE_READER_CONTEXT_ACTION_NAME = "indices:data/read/search[update_context]";

    private final TransportService transportService;
    private final BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper;
    private final Map<String, Long> clientConnections = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    public SearchTransportService(
        TransportService transportService,
        BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper
    ) {
        this.transportService = transportService;
        this.responseWrapper = responseWrapper;
    }

    public void sendFreeContext(Transport.Connection connection, final ShardSearchContextId contextId, OriginalIndices originalIndices) {
        transportService.sendRequest(
            connection,
            FREE_CONTEXT_ACTION_NAME,
            new SearchFreeContextRequest(originalIndices, contextId),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(new ActionListener<SearchFreeContextResponse>() {
                @Override
                public void onResponse(SearchFreeContextResponse response) {
                    // no need to respond if it was freed or not
                }

                @Override
                public void onFailure(Exception e) {

                }
            }, SearchFreeContextResponse::new)
        );
    }

    public void sendFreeContext(
        Transport.Connection connection,
        ShardSearchContextId contextId,
        ActionListener<SearchFreeContextResponse> listener
    ) {
        transportService.sendRequest(
            connection,
            FREE_CONTEXT_SCROLL_ACTION_NAME,
            new ScrollFreeContextRequest(contextId),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, SearchFreeContextResponse::new)
        );
    }

    public void updatePitContext(
        Transport.Connection connection,
        UpdatePitContextRequest request,
        ActionListener<UpdatePitContextResponse> actionListener
    ) {
        transportService.sendRequest(
            connection,
            UPDATE_READER_CONTEXT_ACTION_NAME,
            request,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(actionListener, UpdatePitContextResponse::new)
        );
    }

    public void createPitContext(
        Transport.Connection connection,
        TransportCreatePitAction.CreateReaderContextRequest request,
        SearchTask task,
        ActionListener<TransportCreatePitAction.CreateReaderContextResponse> actionListener
    ) {
        transportService.sendChildRequest(
            connection,
            CREATE_READER_CONTEXT_ACTION_NAME,
            request,
            task,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(actionListener, TransportCreatePitAction.CreateReaderContextResponse::new)
        );
    }

    public void sendCanMatch(
        Transport.Connection connection,
        final ShardSearchRequest request,
        SearchTask task,
        final ActionListener<SearchService.CanMatchResponse> listener
    ) {
        transportService.sendChildRequest(
            connection,
            QUERY_CAN_MATCH_NAME,
            request,
            task,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, SearchService.CanMatchResponse::new)
        );
    }

    public void sendClearAllScrollContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
        transportService.sendRequest(
            connection,
            CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
            TransportRequest.Empty.INSTANCE,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, (in) -> TransportResponse.Empty.INSTANCE)
        );
    }

    public void sendFreePITContexts(
        Transport.Connection connection,
        List<PitSearchContextIdForNode> contextIds,
        ActionListener<DeletePitResponse> listener
    ) {
        transportService.sendRequest(
            connection,
            FREE_PIT_CONTEXT_ACTION_NAME,
            new PitFreeContextsRequest(contextIds),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, DeletePitResponse::new)
        );
    }

    public void sendExecuteDfs(
        Transport.Connection connection,
        final ShardSearchRequest request,
        SearchTask task,
        final SearchActionListener<DfsSearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            DFS_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, DfsSearchResult::new, clientConnections, connection.getNode().getId())
        );
    }

    public void sendExecuteQuery(
        Transport.Connection connection,
        final ShardSearchRequest request,
        SearchTask task,
        final SearchActionListener<SearchPhaseResult> listener
    ) {
        // we optimize this and expect a QueryFetchSearchResult if we only have a single shard in the search request
        // this used to be the QUERY_AND_FETCH which doesn't exist anymore.
        final boolean fetchDocuments = request.numberOfShards() == 1;
        Writeable.Reader<SearchPhaseResult> reader = fetchDocuments ? QueryFetchSearchResult::new : QuerySearchResult::new;

        final ActionListener handler = responseWrapper.apply(connection, listener);
        transportService.sendChildRequest(
            connection,
            QUERY_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(handler, reader, clientConnections, connection.getNode().getId())
        );
    }

    public void sendExecuteQuery(
        Transport.Connection connection,
        final QuerySearchRequest request,
        SearchTask task,
        final SearchActionListener<QuerySearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            QUERY_ID_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, QuerySearchResult::new, clientConnections, connection.getNode().getId())
        );
    }

    public void sendExecuteScrollQuery(
        Transport.Connection connection,
        final InternalScrollSearchRequest request,
        SearchTask task,
        final SearchActionListener<ScrollQuerySearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            QUERY_SCROLL_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, ScrollQuerySearchResult::new, clientConnections, connection.getNode().getId())
        );
    }

    public void sendExecuteScrollFetch(
        Transport.Connection connection,
        final InternalScrollSearchRequest request,
        SearchTask task,
        final SearchActionListener<ScrollQueryFetchSearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            QUERY_FETCH_SCROLL_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, ScrollQueryFetchSearchResult::new, clientConnections, connection.getNode().getId())
        );
    }

    public void sendExecuteFetch(
        Transport.Connection connection,
        final ShardFetchSearchRequest request,
        SearchTask task,
        final SearchActionListener<FetchSearchResult> listener
    ) {
        sendExecuteFetch(connection, FETCH_ID_ACTION_NAME, request, task, listener);
    }

    public void sendExecuteFetchScroll(
        Transport.Connection connection,
        final ShardFetchRequest request,
        SearchTask task,
        final SearchActionListener<FetchSearchResult> listener
    ) {
        sendExecuteFetch(connection, FETCH_ID_SCROLL_ACTION_NAME, request, task, listener);
    }

    private void sendExecuteFetch(
        Transport.Connection connection,
        String action,
        final ShardFetchRequest request,
        SearchTask task,
        final SearchActionListener<FetchSearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            action,
            request,
            task,
            new ConnectionCountingHandler<>(listener, FetchSearchResult::new, clientConnections, connection.getNode().getId())
        );
    }

    /**
     * Used by {@link TransportSearchAction} to send the expand queries (field collapsing).
     */
    void sendExecuteMultiSearch(final MultiSearchRequest request, SearchTask task, final ActionListener<MultiSearchResponse> listener) {
        final Transport.Connection connection = transportService.getConnection(transportService.getLocalNode());
        transportService.sendChildRequest(
            connection,
            MultiSearchAction.NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, MultiSearchResponse::new, clientConnections, connection.getNode().getId())
        );
    }

    public RemoteClusterService getRemoteClusterService() {
        return transportService.getRemoteClusterService();
    }

    /**
     * Return a map of nodeId to pending number of search requests.
     * This is a snapshot of the current pending search and not a live map.
     */
    public Map<String, Long> getPendingSearchRequests() {
        return new HashMap<>(clientConnections);
    }

    /**
     * A scroll free context request
     *
     * @opensearch.internal
     */
    static class ScrollFreeContextRequest extends TransportRequest {
        private ShardSearchContextId contextId;

        ScrollFreeContextRequest(ShardSearchContextId contextId) {
            this.contextId = Objects.requireNonNull(contextId);
        }

        ScrollFreeContextRequest(StreamInput in) throws IOException {
            super(in);
            contextId = new ShardSearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            contextId.writeTo(out);
        }

        public ShardSearchContextId id() {
            return this.contextId;
        }

    }

    /**
     * Request to free the PIT context based on id
     */
    static class PitFreeContextsRequest extends TransportRequest {
        private List<PitSearchContextIdForNode> contextIds;

        PitFreeContextsRequest(List<PitSearchContextIdForNode> contextIds) {
            this.contextIds = new ArrayList<>();
            this.contextIds.addAll(contextIds);
        }

        PitFreeContextsRequest(StreamInput in) throws IOException {
            super(in);
            int size = in.readVInt();
            if (size > 0) {
                this.contextIds = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    PitSearchContextIdForNode contextId = new PitSearchContextIdForNode(in);
                    contextIds.add(contextId);
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(contextIds.size());
            for (PitSearchContextIdForNode contextId : contextIds) {
                contextId.writeTo(out);
            }
        }

        public List<PitSearchContextIdForNode> getContextIds() {
            return this.contextIds;
        }
    }

    /**
     * A search free context request
     *
     * @opensearch.internal
     */
    static class SearchFreeContextRequest extends ScrollFreeContextRequest implements IndicesRequest {
        private OriginalIndices originalIndices;

        SearchFreeContextRequest(OriginalIndices originalIndices, ShardSearchContextId id) {
            super(id);
            this.originalIndices = originalIndices;
        }

        SearchFreeContextRequest(StreamInput in) throws IOException {
            super(in);
            originalIndices = OriginalIndices.readOriginalIndices(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
        }

        @Override
        public String[] indices() {
            if (originalIndices == null) {
                return null;
            }
            return originalIndices.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            if (originalIndices == null) {
                return null;
            }
            return originalIndices.indicesOptions();
        }

    }

    /**
     * A search free context response
     *
     * @opensearch.internal
     */
    public static class SearchFreeContextResponse extends TransportResponse {

        private boolean freed;

        SearchFreeContextResponse(StreamInput in) throws IOException {
            freed = in.readBoolean();
        }

        SearchFreeContextResponse(boolean freed) {
            this.freed = freed;
        }

        public boolean isFreed() {
            return freed;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(freed);
        }
    }

    public static void registerRequestHandler(TransportService transportService, SearchService searchService) {
        transportService.registerRequestHandler(
            FREE_CONTEXT_SCROLL_ACTION_NAME,
            ThreadPool.Names.SAME,
            ScrollFreeContextRequest::new,
            (request, channel, task) -> {
                boolean freed = searchService.freeReaderContext(request.id());
                channel.sendResponse(new SearchFreeContextResponse(freed));
            }
        );
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_SCROLL_ACTION_NAME, SearchFreeContextResponse::new);

        transportService.registerRequestHandler(
            FREE_PIT_CONTEXT_ACTION_NAME,
            ThreadPool.Names.SAME,
            PitFreeContextsRequest::new,
            (request, channel, task) -> {
                channel.sendResponse(searchService.freeReaderContextsIfFound(request.getContextIds()));
            }
        );
        TransportActionProxy.registerProxyAction(transportService, FREE_PIT_CONTEXT_ACTION_NAME, DeletePitResponse::new);

        transportService.registerRequestHandler(
            FREE_CONTEXT_ACTION_NAME,
            ThreadPool.Names.SAME,
            SearchFreeContextRequest::new,
            (request, channel, task) -> {
                boolean freed = searchService.freeReaderContext(request.id());
                channel.sendResponse(new SearchFreeContextResponse(freed));
            }
        );
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_ACTION_NAME, SearchFreeContextResponse::new);
        transportService.registerRequestHandler(
            CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
            ThreadPool.Names.SAME,
            TransportRequest.Empty::new,
            (request, channel, task) -> {
                searchService.freeAllScrollContexts();
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
            (in) -> TransportResponse.Empty.INSTANCE
        );

        transportService.registerRequestHandler(
            DFS_ACTION_NAME,
            ThreadPool.Names.SAME,
            ShardSearchRequest::new,
            (request, channel, task) -> searchService.executeDfsPhase(
                request,
                false,
                (SearchShardTask) task,
                new ChannelActionListener<>(channel, DFS_ACTION_NAME, request)
            )
        );

        TransportActionProxy.registerProxyAction(transportService, DFS_ACTION_NAME, DfsSearchResult::new);

        transportService.registerRequestHandler(
            QUERY_ACTION_NAME,
            ThreadPool.Names.SAME,
            ShardSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(
                    request,
                    false,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_ACTION_NAME, request)
                );
            }
        );
        TransportActionProxy.registerProxyActionWithDynamicResponseType(
            transportService,
            QUERY_ACTION_NAME,
            (request) -> ((ShardSearchRequest) request).numberOfShards() == 1 ? QueryFetchSearchResult::new : QuerySearchResult::new
        );

        transportService.registerRequestHandler(
            QUERY_ID_ACTION_NAME,
            ThreadPool.Names.SAME,
            QuerySearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_ID_ACTION_NAME, request)
                );
            }
        );
        TransportActionProxy.registerProxyAction(transportService, QUERY_ID_ACTION_NAME, QuerySearchResult::new);

        transportService.registerRequestHandler(
            QUERY_SCROLL_ACTION_NAME,
            ThreadPool.Names.SAME,
            InternalScrollSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_SCROLL_ACTION_NAME, request)
                );
            }
        );
        TransportActionProxy.registerProxyAction(transportService, QUERY_SCROLL_ACTION_NAME, ScrollQuerySearchResult::new);

        transportService.registerRequestHandler(
            QUERY_FETCH_SCROLL_ACTION_NAME,
            ThreadPool.Names.SAME,
            InternalScrollSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_FETCH_SCROLL_ACTION_NAME, request)
                );
            }
        );
        TransportActionProxy.registerProxyAction(transportService, QUERY_FETCH_SCROLL_ACTION_NAME, ScrollQueryFetchSearchResult::new);

        transportService.registerRequestHandler(
            FETCH_ID_SCROLL_ACTION_NAME,
            ThreadPool.Names.SAME,
            ShardFetchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel, FETCH_ID_SCROLL_ACTION_NAME, request)
                );
            }
        );
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_SCROLL_ACTION_NAME, FetchSearchResult::new);

        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            ThreadPool.Names.SAME,
            true,
            true,
            ShardFetchSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel, FETCH_ID_ACTION_NAME, request)
                );
            }
        );
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_ACTION_NAME, FetchSearchResult::new);

        // this is cheap, it does not fetch during the rewrite phase, so we can let it quickly execute on a networking thread
        transportService.registerRequestHandler(
            QUERY_CAN_MATCH_NAME,
            ThreadPool.Names.SAME,
            ShardSearchRequest::new,
            (request, channel, task) -> {
                searchService.canMatch(request, new ChannelActionListener<>(channel, QUERY_CAN_MATCH_NAME, request));
            }
        );
        TransportActionProxy.registerProxyAction(transportService, QUERY_CAN_MATCH_NAME, SearchService.CanMatchResponse::new);
        transportService.registerRequestHandler(
            CREATE_READER_CONTEXT_ACTION_NAME,
            ThreadPool.Names.SAME,
            TransportCreatePitAction.CreateReaderContextRequest::new,
            (request, channel, task) -> {
                ChannelActionListener<
                    TransportCreatePitAction.CreateReaderContextResponse,
                    TransportCreatePitAction.CreateReaderContextRequest> listener = new ChannelActionListener<>(
                        channel,
                        CREATE_READER_CONTEXT_ACTION_NAME,
                        request
                    );
                searchService.createPitReaderContext(
                    request.getShardId(),
                    request.getKeepAlive(),
                    ActionListener.wrap(
                        r -> listener.onResponse(new TransportCreatePitAction.CreateReaderContextResponse(r)),
                        listener::onFailure
                    )
                );
            }
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            CREATE_READER_CONTEXT_ACTION_NAME,
            TransportCreatePitAction.CreateReaderContextResponse::new
        );

        transportService.registerRequestHandler(
            UPDATE_READER_CONTEXT_ACTION_NAME,
            ThreadPool.Names.SAME,
            UpdatePitContextRequest::new,
            (request, channel, task) -> {
                ChannelActionListener<UpdatePitContextResponse, UpdatePitContextRequest> listener = new ChannelActionListener<>(
                    channel,
                    UPDATE_READER_CONTEXT_ACTION_NAME,
                    request
                );
                searchService.updatePitIdAndKeepAlive(request, listener);
            }
        );
        TransportActionProxy.registerProxyAction(transportService, UPDATE_READER_CONTEXT_ACTION_NAME, UpdatePitContextResponse::new);
    }

    /**
     * Returns a connection to the given node on the provided cluster. If the cluster alias is <code>null</code> the node will be resolved
     * against the local cluster.
     * @param clusterAlias the cluster alias the node should be resolved against
     * @param node the node to resolve
     * @return a connection to the given node belonging to the cluster with the provided alias.
     */
    public Transport.Connection getConnection(@Nullable String clusterAlias, DiscoveryNode node) {
        if (clusterAlias == null) {
            return transportService.getConnection(node);
        } else {
            return transportService.getRemoteClusterService().getConnection(node, clusterAlias);
        }
    }

    /**
     * A handler that counts connections
     *
     * @opensearch.internal
     */
    final class ConnectionCountingHandler<Response extends TransportResponse> extends ActionListenerResponseHandler<Response> {
        private final Map<String, Long> clientConnections;
        private final String nodeId;

        ConnectionCountingHandler(
            final ActionListener<? super Response> listener,
            final Writeable.Reader<Response> responseReader,
            final Map<String, Long> clientConnections,
            final String nodeId
        ) {
            super(listener, responseReader);
            this.clientConnections = clientConnections;
            this.nodeId = nodeId;
            // Increment the number of connections for this node by one
            clientConnections.compute(nodeId, (id, conns) -> conns == null ? 1 : conns + 1);
        }

        @Override
        public void handleResponse(Response response) {
            super.handleResponse(response);
            // Decrement the number of connections or remove it entirely if there are no more connections
            // We need to remove the entry here so we don't leak when nodes go away forever
            assert assertNodePresent();
            clientConnections.computeIfPresent(nodeId, (id, conns) -> conns.longValue() == 1 ? null : conns - 1);
        }

        @Override
        public void handleException(TransportException e) {
            super.handleException(e);
            // Decrement the number of connections or remove it entirely if there are no more connections
            // We need to remove the entry here so we don't leak when nodes go away forever
            assert assertNodePresent();
            clientConnections.computeIfPresent(nodeId, (id, conns) -> conns.longValue() == 1 ? null : conns - 1);
        }

        private boolean assertNodePresent() {
            clientConnections.compute(nodeId, (id, conns) -> {
                assert conns != null : "number of connections for " + id + " is null, but should be an integer";
                assert conns >= 1 : "number of connections for " + id + " should be >= 1 but was " + conns;
                return conns;
            });
            // Always return true, there is additional asserting here, the boolean is just so this
            // can be skipped when assertions are not enabled
            return true;
        }
    }
}
