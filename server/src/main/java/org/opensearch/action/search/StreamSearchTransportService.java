/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.StreamChannelActionListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.QueryFetchSearchResult;
import org.opensearch.search.fetch.ShardFetchSearchRequest;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.function.BiFunction;

public class StreamSearchTransportService extends SearchTransportService {
    private final StreamTransportService transportService;

    public StreamSearchTransportService(
        StreamTransportService transportService,
        BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper
    ) {
        super(transportService, responseWrapper);
        this.transportService = transportService;
    }

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
                    new StreamChannelActionListener<>(channel, QUERY_ACTION_NAME, request)
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
                    new StreamChannelActionListener<>(channel, FETCH_ID_ACTION_NAME, request)
                );
            }
        );
    }

    @Override
    public void sendExecuteQuery(
        Transport.Connection connection,
        final ShardSearchRequest request,
        SearchTask task,
        final SearchActionListener<SearchPhaseResult> listener
    ) {
        final boolean fetchDocuments = request.numberOfShards() == 1;
        Writeable.Reader<SearchPhaseResult> reader = fetchDocuments ? QueryFetchSearchResult::new : QuerySearchResult::new;

        TransportResponseHandler<SearchPhaseResult> transportHandler = new TransportResponseHandler<SearchPhaseResult>() {

            @Override
            public void handleStreamResponse(StreamTransportResponse<SearchPhaseResult> response) {
                try {
                    SearchPhaseResult result = response.nextResponse();
                    listener.onResponse(result);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void handleResponse(SearchPhaseResult response) {
                throw new IllegalStateException("handleResponse is not supported for Streams");
            }

            @Override
            public void handleException(TransportException e) {
                listener.onFailure(e);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SEARCH;
            } // TODO: use a different thread pool for stream

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
            transportHandler // TODO: check feasibility of ConnectionCountingHandler
        );
    }

    @Override
    public void sendExecuteFetch(
        Transport.Connection connection,
        final ShardFetchSearchRequest request,
        SearchTask task,
        final SearchActionListener<FetchSearchResult> listener
    ) {
        TransportResponseHandler<FetchSearchResult> transportHandler = new TransportResponseHandler<FetchSearchResult>() {

            @Override
            public void handleStreamResponse(StreamTransportResponse<FetchSearchResult> response) {
                FetchSearchResult result = response.nextResponse();
                listener.onResponse(result);
            }

            @Override
            public void handleResponse(FetchSearchResult response) {
                throw new IllegalStateException("handleResponse is not supported for Streams");
            }

            @Override
            public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SEARCH;
            } // TODO: use a different thread pool for stream

            @Override
            public FetchSearchResult read(StreamInput in) throws IOException {
                return new FetchSearchResult(in);
            }
        };
        transportService.sendChildRequest(connection, FETCH_ID_ACTION_NAME, request, task, transportHandler);
    }
}
