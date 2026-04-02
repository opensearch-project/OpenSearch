/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamSearchTransportServiceTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testSendExecuteQueryEmitsIntermediateBeforeFinal() throws Exception {
        StreamTransportService transportService = mock(StreamTransportService.class);
        Transport.Connection connection = mock(Transport.Connection.class);
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.numberOfShards()).thenReturn(2);

        List<String> events = new ArrayList<>();
        StreamSearchActionListener<SearchPhaseResult> listener = new StreamSearchActionListener<>(null, 0) {
            @Override
            protected void innerOnStreamResponse(SearchPhaseResult response) {
                events.add("stream");
            }

            @Override
            protected void innerOnCompleteResponse(SearchPhaseResult response) {
                events.add("final");
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        };

        QuerySearchResult first = new QuerySearchResult();
        QuerySearchResult second = new QuerySearchResult();
        QuerySearchResult third = new QuerySearchResult();
        StreamTransportResponse<SearchPhaseResult> streamResponse = mock(StreamTransportResponse.class);
        when(streamResponse.nextResponse()).thenReturn(first, second, third, null);

        doAnswer(invocation -> {
            TransportResponseHandler<SearchPhaseResult> responseHandler = invocation.getArgument(4);
            ((StreamTransportResponseHandler<SearchPhaseResult>) responseHandler).handleStreamResponse(streamResponse);
            return null;
        }).when(transportService)
            .sendChildRequest(
                eq(connection),
                eq(SearchTransportService.QUERY_ACTION_NAME),
                eq(request),
                any(),
                any(TransportResponseHandler.class)
            );

        BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper = (conn, l) -> l;
        StreamSearchTransportService service = new StreamSearchTransportService(transportService, responseWrapper);

        service.sendExecuteQuery(connection, request, null, listener);

        assertEquals(List.of("stream", "stream", "final"), events);
        verify(streamResponse, times(1)).close();
    }
}
