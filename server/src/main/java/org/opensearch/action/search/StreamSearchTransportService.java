/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.QueryFetchSearchResult;
import org.opensearch.search.fetch.ShardFetchSearchRequest;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Search transport service for streaming search
 *
 * @opensearch.internal
 */
public class StreamSearchTransportService extends SearchTransportService {
    private final Logger logger = LogManager.getLogger(StreamSearchTransportService.class);

    private final StreamTransportService transportService;

    public StreamSearchTransportService(
        StreamTransportService transportService,
        BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper
    ) {
        super(transportService, responseWrapper);
        this.transportService = transportService;
    }

    public static final Setting<Boolean> STREAM_SEARCH_ENABLED = Setting.boolSetting(
        "stream.search.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static void registerStreamRequestHandler(StreamTransportService transportService, SearchService searchService) {
        transportService.registerRequestHandler(
            QUERY_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            true,
            AdmissionControlActionType.SEARCH,
            ShardSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(
                    request,
                    false,
                    (SearchShardTask) task,
                    new StreamSearchChannelListener<>(channel, QUERY_ACTION_NAME, request),
                    ThreadPool.Names.STREAM_SEARCH,
                    true
                );
            }
        );
        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            ThreadPool.Names.SAME,
            true,
            true,
            AdmissionControlActionType.SEARCH,
            ShardFetchSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(
                    request,
                    (SearchShardTask) task,
                    new StreamSearchChannelListener<>(channel, FETCH_ID_ACTION_NAME, request),
                    ThreadPool.Names.STREAM_SEARCH
                );
            }
        );
        transportService.registerRequestHandler(
            QUERY_CAN_MATCH_NAME,
            ThreadPool.Names.SAME,
            ShardSearchRequest::new,
            (request, channel, task) -> {
                searchService.canMatch(request, new StreamSearchChannelListener<>(channel, QUERY_CAN_MATCH_NAME, request));
            }
        );
        transportService.registerRequestHandler(
            FREE_CONTEXT_ACTION_NAME,
            ThreadPool.Names.SAME,
            SearchFreeContextRequest::new,
            (request, channel, task) -> {
                boolean freed = searchService.freeReaderContext(request.id());
                channel.sendResponseBatch(new SearchFreeContextResponse(freed));
                channel.completeStream();
            }
        );

        transportService.registerRequestHandler(
            DFS_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            true,
            AdmissionControlActionType.SEARCH,
            ShardSearchRequest::new,
            (request, channel, task) -> searchService.executeDfsPhase(
                request,
                false,
                (SearchShardTask) task,
                new StreamSearchChannelListener<>(channel, DFS_ACTION_NAME, request),
                ThreadPool.Names.STREAM_SEARCH
            )
        );
    }

    @Override
    public void sendExecuteQuery(
        Transport.Connection connection,
        final ShardSearchRequest request,
        SearchTask task,
        SearchActionListener<SearchPhaseResult> listener
    ) {
        final boolean fetchDocuments = request.numberOfShards() == 1;
        Writeable.Reader<SearchPhaseResult> reader = fetchDocuments ? QueryFetchSearchResult::new : QuerySearchResult::new;

        final StreamSearchActionListener streamListener = (StreamSearchActionListener) listener;
        StreamTransportResponseHandler<SearchPhaseResult> transportHandler = new StreamTransportResponseHandler<SearchPhaseResult>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<SearchPhaseResult> response) {
                try {
                    // only send previous result if we have a current result
                    // if current result is null, that means the previous result is the last result
                    SearchPhaseResult currentResult;
                    SearchPhaseResult lastResult = null;

                    // Keep reading results until we reach the end
                    while ((currentResult = response.nextResponse()) != null) {
                        if (lastResult != null) {
                            streamListener.onStreamResponse(lastResult, false);
                        }
                        lastResult = currentResult;
                    }

                    // Send the final result as complete response, or null if no results
                    if (lastResult != null) {
                        streamListener.onStreamResponse(lastResult, true);
                        logger.debug("Processed final stream response");
                    } else {
                        // Empty stream case
                        logger.error("Empty stream");
                    }
                    response.close();
                } catch (Exception e) {
                    response.cancel("Client error during search phase", e);
                    streamListener.onFailure(e);
                }
            }

            @Override
            public void handleException(TransportException e) {
                listener.onFailure(e);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.STREAM_SEARCH;
            }

            @Override
            public SearchPhaseResult read(StreamInput in) throws IOException {
                return reader.read(in);
            }
        };

        transportService.sendChildRequest(
            connection,
            QUERY_ACTION_NAME,
            request,
            task,
            transportHandler // TODO: wrap with ConnectionCountingHandler
        );
    }

    @Override
    public void sendExecuteFetch(
        Transport.Connection connection,
        final ShardFetchSearchRequest request,
        SearchTask task,
        final SearchActionListener<FetchSearchResult> listener
    ) {
        StreamTransportResponseHandler<FetchSearchResult> transportHandler = new StreamTransportResponseHandler<FetchSearchResult>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<FetchSearchResult> response) {
                try {
                    FetchSearchResult result = response.nextResponse();
                    listener.onResponse(result);
                    response.close();
                } catch (Exception e) {
                    response.cancel("Client error during fetch phase", e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.STREAM_SEARCH;
            }

            @Override
            public FetchSearchResult read(StreamInput in) throws IOException {
                return new FetchSearchResult(in);
            }
        };
        transportService.sendChildRequest(connection, FETCH_ID_ACTION_NAME, request, task, transportHandler);
    }

    @Override
    public void sendCanMatch(
        Transport.Connection connection,
        final ShardSearchRequest request,
        SearchTask task,
        final ActionListener<SearchService.CanMatchResponse> listener
    ) {
        StreamTransportResponseHandler<SearchService.CanMatchResponse> transportHandler = new StreamTransportResponseHandler<
            SearchService.CanMatchResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<SearchService.CanMatchResponse> response) {
                try {
                    SearchService.CanMatchResponse result = response.nextResponse();
                    if (response.nextResponse() != null) {
                        throw new IllegalStateException("Only one response expected from SearchService.CanMatchResponse");
                    }
                    listener.onResponse(result);
                    response.close();
                } catch (Exception e) {
                    response.cancel("Client error during can match", e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public SearchService.CanMatchResponse read(StreamInput in) throws IOException {
                return new SearchService.CanMatchResponse(in);
            }
        };

        transportService.sendChildRequest(
            connection,
            QUERY_CAN_MATCH_NAME,
            request,
            task,
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            transportHandler
        );
    }

    @Override
    public void sendFreeContext(Transport.Connection connection, final ShardSearchContextId contextId, OriginalIndices originalIndices) {
        StreamTransportResponseHandler<SearchFreeContextResponse> transportHandler = new StreamTransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<SearchFreeContextResponse> response) {
                try {
                    response.nextResponse();
                    response.close();
                } catch (Exception ignore) {

                }
            }

            @Override
            public void handleException(TransportException exp) {

            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public SearchFreeContextResponse read(StreamInput in) throws IOException {
                return new SearchFreeContextResponse(in);
            }
        };
        transportService.sendRequest(
            connection,
            FREE_CONTEXT_ACTION_NAME,
            new SearchFreeContextRequest(originalIndices, contextId),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            transportHandler
        );
    }

    @Override
    public void sendExecuteDfs(
        Transport.Connection connection,
        final ShardSearchRequest request,
        SearchTask task,
        final SearchActionListener<DfsSearchResult> listener
    ) {
        StreamTransportResponseHandler<DfsSearchResult> transportHandler = new StreamTransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<DfsSearchResult> response) {
                try {
                    DfsSearchResult result = response.nextResponse();
                    listener.onResponse(result);
                    response.close();
                } catch (Exception e) {
                    response.cancel("Client error during search phase", e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void handleException(TransportException e) {
                listener.onFailure(e);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.STREAM_SEARCH;
            }

            @Override
            public DfsSearchResult read(StreamInput in) throws IOException {
                return new DfsSearchResult(in);
            }
        };

        transportService.sendChildRequest(
            connection,
            DFS_ACTION_NAME,
            request,
            task,
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            transportHandler
        );
    }
}
