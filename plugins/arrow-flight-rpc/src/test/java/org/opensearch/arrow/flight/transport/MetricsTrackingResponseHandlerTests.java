/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * The metrics wrapper sits between the transport and the consumer, so it has to pass both halves of the
 * stream-cancellation contract through: the {@code onStreamCreated} hand-off, and a cancellation that
 * does not close the stream.
 */
public class MetricsTrackingResponseHandlerTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testOnStreamCreatedIsForwardedToTheDelegate() {
        TransportResponseHandler<TransportResponse> delegate = mock(TransportResponseHandler.class);
        FlightTransportResponse<TransportResponse> response = mock(FlightTransportResponse.class);
        MetricsTrackingResponseHandler<TransportResponse> handler = newHandler(delegate);

        handler.onStreamCreated(response);

        verify(delegate).onStreamCreated(response);
    }

    /**
     * The wrapper must override {@code cancelStreamOnly} rather than inherit the interface default, which
     * cancels <em>and</em> closes: this call arrives from a thread other than the consumer, and closing
     * would free the batch that consumer is reading.
     */
    @SuppressWarnings("unchecked")
    public void testCancelStreamOnlyIsForwardedWithoutClosingTheStream() throws Exception {
        FlightTransportResponse<TransportResponse> response = mock(FlightTransportResponse.class);
        AtomicReference<StreamTransportResponse<TransportResponse>> handedToConsumer = new AtomicReference<>();
        MetricsTrackingResponseHandler<TransportResponse> handler = newHandler(recordingHandler(handedToConsumer));

        handler.handleStreamResponse(response);
        assertNotNull("consumer must have been handed a stream", handedToConsumer.get());

        handedToConsumer.get().cancelStreamOnly("analytics query task cancelled");

        verify(response).cancelStreamOnly("analytics query task cancelled");
        verify(response, never()).cancel(anyString(), any());
        verify(response, never()).close();
    }

    private static MetricsTrackingResponseHandler<TransportResponse> newHandler(TransportResponseHandler<TransportResponse> delegate) {
        return new MetricsTrackingResponseHandler<>(delegate, new FlightStatsCollector().createClientCallTracker());
    }

    /** A consumer that only captures the stream it is given, so the test can act on it afterwards. */
    private static TransportResponseHandler<TransportResponse> recordingHandler(
        AtomicReference<StreamTransportResponse<TransportResponse>> sink
    ) {
        return new TransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TransportResponse> streamResponse) {
                sink.set(streamResponse);
            }

            @Override
            public void handleResponse(TransportResponse response) {
                throw new AssertionError("unexpected unary response");
            }

            @Override
            public void handleException(TransportException exp) {
                throw new AssertionError("unexpected exception", exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TransportResponse read(StreamInput in) throws IOException {
                throw new AssertionError("read must not be called");
            }
        };
    }
}
