/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.channels;

import org.opensearch.core.transport.TransportResponse;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.FakeTcpChannel;
import org.opensearch.transport.TcpTransportChannel;
import org.opensearch.transport.TransportChannel;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TraceableTcpTransportChannel}
 */
public class TraceableTcpTransportChannelTests extends OpenSearchTestCase {

    private FakeTcpChannel tcpChannel;
    private TcpTransportChannel delegate;
    private Span span;
    private Tracer tracer;
    private SpanScope spanScope;

    @Before
    public void setup() {
        tcpChannel = new FakeTcpChannel();
        delegate = mock(TcpTransportChannel.class);
        span = mock(Span.class);
        tracer = mock(Tracer.class);
        spanScope = mock(SpanScope.class);

        when(delegate.getChannel()).thenReturn(tcpChannel);
        when(tracer.isRecording()).thenReturn(true);
        when(tracer.withSpanInScope(span)).thenReturn(spanScope);
    }

    public void testCreateReturnsTracingChannelWhenRecording() {
        TransportChannel channel = TraceableTcpTransportChannel.create(delegate, span, tracer);

        assertThat(channel, instanceOf(TraceableTcpTransportChannel.class));
    }

    public void testCreateReturnsDelegateWhenNotRecording() {
        when(tracer.isRecording()).thenReturn(false);

        TransportChannel channel = TraceableTcpTransportChannel.create(delegate, span, tracer);

        assertSame(delegate, channel);
        assertEquals(0, tcpChannel.numberOfCloseListeners());
    }

    public void testSendResponseEndsSpanWithinScope() throws IOException {
        TransportChannel channel = TraceableTcpTransportChannel.create(delegate, span, tracer);
        TransportResponse response = mock(TransportResponse.class);

        channel.sendResponse(response);

        verify(delegate, times(1)).sendResponse(response);
        verify(tracer, times(1)).withSpanInScope(span);
        verify(spanScope, times(1)).close();
        verify(span, times(1)).endSpan();
    }

    public void testSendErrorResponseMarksSpanAsErrorAndEndsIt() throws IOException {
        TransportChannel channel = TraceableTcpTransportChannel.create(delegate, span, tracer);
        Exception exception = new IOException("something went wrong");

        channel.sendResponse(exception);

        verify(delegate, times(1)).sendResponse(exception);
        verify(span, times(1)).setError(exception);
        verify(span, times(1)).endSpan();
    }

    public void testCloseListenerIsGivenBackOnceTheResponseIsSent() throws IOException {
        TransportChannel channel = TraceableTcpTransportChannel.create(delegate, span, tracer);
        assertEquals(1, tcpChannel.numberOfCloseListeners());

        channel.sendResponse(mock(TransportResponse.class));

        assertEquals(0, tcpChannel.numberOfCloseListeners());
    }

    public void testCloseListenersDoNotAccumulateOverRequests() throws IOException {
        // a transport connection carries every request between two nodes and is only closed when a node goes away,
        // so a listener registered per request must not be left behind on it
        int requests = randomIntBetween(2, 100);
        List<Span> spans = new ArrayList<>(requests);
        for (int i = 0; i < requests; i++) {
            Span requestSpan = mock(Span.class);
            spans.add(requestSpan);
            TraceableTcpTransportChannel.create(delegate, requestSpan, tracer).sendResponse(mock(TransportResponse.class));
        }

        assertEquals(0, tcpChannel.numberOfCloseListeners());

        // those spans are done, so closing the connection must not touch them again
        tcpChannel.close();
        for (Span requestSpan : spans) {
            verify(requestSpan, times(1)).endSpan();
        }
    }

    public void testSpanIsEndedWhenTheChannelIsClosedWithoutAResponse() {
        TraceableTcpTransportChannel.create(delegate, span, tracer);

        tcpChannel.close();

        verify(span, times(1)).addEvent("The TransportChannel was closed without sending the response");
        verify(span, times(1)).setError(null);
        verify(span, times(1)).endSpan();
        assertEquals(0, tcpChannel.numberOfCloseListeners());
    }

    public void testSpanIsEndedWhenTheChannelIsAlreadyClosed() {
        tcpChannel.close();

        TraceableTcpTransportChannel.create(delegate, span, tracer);

        verify(span, times(1)).addEvent("The TransportChannel was closed without sending the response");
        verify(span, times(1)).endSpan();
    }
}
