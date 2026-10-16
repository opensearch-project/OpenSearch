/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.handler.TraceableTransportResponseHandler;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Every wrapper on the response-handler chain must forward
 * {@link TransportResponseHandler#onStreamCreated}. That callback is where a consumer arranges to cancel
 * a stream which may never produce a first batch; a wrapper that drops it does not fail anything visibly,
 * it just leaves the request permanently uncancellable.
 */
public class TransportResponseHandlerWrapperTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testContextRestoreHandlerForwardsOnStreamCreated() {
        TransportResponseHandler<TransportResponse> delegate = mock(TransportResponseHandler.class);
        StreamTransportResponse<TransportResponse> stream = mock(StreamTransportResponse.class);
        AtomicInteger contextRestores = new AtomicInteger();

        TransportResponseHandler<TransportResponse> handler = new TransportService.ContextRestoreResponseHandler<>(() -> {
            contextRestores.incrementAndGet();
            return mock(ThreadContext.StoredContext.class);
        }, delegate);

        handler.onStreamCreated(stream);

        verify(delegate).onStreamCreated(stream);
        assertEquals("onStreamCreated runs on the sending thread, whose context is already the caller's", 0, contextRestores.get());
    }

    @SuppressWarnings("unchecked")
    public void testTracingHandlerForwardsOnStreamCreatedWithoutEndingTheSpan() {
        TransportResponseHandler<TransportResponse> delegate = mock(TransportResponseHandler.class);
        StreamTransportResponse<TransportResponse> stream = mock(StreamTransportResponse.class);
        Span span = mock(Span.class);
        Tracer tracer = mock(Tracer.class);
        when(tracer.isRecording()).thenReturn(true);

        TransportResponseHandler<TransportResponse> handler = TraceableTransportResponseHandler.create(delegate, span, tracer);

        handler.onStreamCreated(stream);

        verify(delegate).onStreamCreated(stream);
        verify(span, never()).endSpan();
    }
}
