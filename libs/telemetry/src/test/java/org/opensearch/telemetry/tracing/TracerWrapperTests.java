/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.listeners.TraceEventListener;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.verifyNoInteractions;

public class TracerWrapperTests extends OpenSearchTestCase {

    private Tracer tracer;
    private TraceEventsService traceEventsService;
    private TraceEventListener traceEventListener1;
    private TraceEventListener traceEventListener2;
    private Span span;

    private SpanContext spanContext;

    private SpanScope spanScope;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        tracer = mock(Tracer.class);
        traceEventsService = spy(new TraceEventsService());
        traceEventListener1 = mock(TraceEventListener.class);
        traceEventListener2 = mock(TraceEventListener.class);
        span = mock(Span.class);
        spanContext = new SpanContext(span);

        spanScope = mock(SpanScope.class);

        when(tracer.startSpan(anyString())).thenReturn(spanScope);
        when(tracer.startSpan(anyString(), any())).thenReturn(spanScope);
        when(tracer.getCurrentSpan()).thenReturn(spanContext);
        when(traceEventsService.isTracingEnabled()).thenReturn(true);
        when(traceEventsService.getTracer()).thenReturn(tracer);

        traceEventsService.registerTraceEventListener("listener1", traceEventListener1);
        traceEventsService.registerTraceEventListener("listener2", traceEventListener2);
    }

    public void testStartSpan_WithTracingEnabled_InvokeOnSpanStartAndOnSpanComplete() {
        TracerWrapper tracerWrapper = new TracerWrapper(tracer, traceEventsService);
        when(traceEventListener1.isEnabled(any(Span.class))).thenReturn(true);
        when(traceEventListener2.isEnabled(any(Span.class))).thenReturn(true);

        SpanScope scope = tracerWrapper.startSpan("test_span");

        verify(traceEventListener1).onSpanStart(eq(span), any(Thread.class));
        verify(traceEventListener2).onSpanStart(eq(span), any(Thread.class));

        scope.close();

        verify(traceEventListener1).onSpanComplete(eq(span), any(Thread.class));
        verify(traceEventListener2).onSpanComplete(eq(span), any(Thread.class));
    }

    public void testStartSpan_WithTracingDisabled_NoInteractionsWithListeners() {
        when(traceEventsService.isTracingEnabled()).thenReturn(false);

        TracerWrapper tracerWrapper = new TracerWrapper(tracer, traceEventsService);
        SpanScope scope = tracerWrapper.startSpan("test_span");

        scope.close();

        verifyNoInteractions(traceEventListener1);
        verifyNoInteractions(traceEventListener2);
    }

    public void testUnwrap() {
        TracerWrapper tracerWrapper = new TracerWrapper(tracer, traceEventsService);
        Tracer unwrappedTracer = tracerWrapper.unwrap();
        assertSame(tracer, unwrappedTracer);
    }
}
