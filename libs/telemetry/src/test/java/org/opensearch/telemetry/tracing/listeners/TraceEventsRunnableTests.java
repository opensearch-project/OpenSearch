/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listeners;

import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;

import org.mockito.Mockito;

public class TraceEventsRunnableTests extends OpenSearchTestCase {

    private Tracer tracer;
    private TraceEventsService traceEventsService;
    private TraceEventListener traceEventListener1;
    private TraceEventListener traceEventListener2;
    private Span span;
    private Thread currentThread;
    private Runnable delegate;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        tracer = Mockito.mock(Tracer.class);
        traceEventsService = Mockito.spy(new TraceEventsService()); // Use spy here
        traceEventListener1 = Mockito.mock(TraceEventListener.class);
        traceEventListener2 = Mockito.mock(TraceEventListener.class);
        span = Mockito.mock(Span.class);
        currentThread = Mockito.mock(Thread.class);
        delegate = Mockito.mock(Runnable.class);

        Mockito.when(traceEventsService.getTracer()).thenReturn(tracer);
        Mockito.when(tracer.getCurrentSpan()).thenReturn(span);
        Mockito.when(span.getParentSpan()).thenReturn(null);

        traceEventsService.registerTraceEventListener("listener1", traceEventListener1);
        traceEventsService.registerTraceEventListener("listener2", traceEventListener2);
        Mockito.when(traceEventListener1.isEnabled(Mockito.any(Span.class))).thenReturn(true);
        Mockito.when(traceEventListener2.isEnabled(Mockito.any(Span.class))).thenReturn(true);

        traceEventsService.setTracingEnabled(true);
    }

    public void testRun_InvokeOnRunnableStartAndOnRunnableComplete() {
        Span span1 = Mockito.mock(Span.class);
        Span span2 = Mockito.mock(Span.class);
        Mockito.when(traceEventsService.getTracer().getCurrentSpan()).thenReturn(span1, span1);
        Mockito.when(span1.hasEnded()).thenReturn(false);
        Mockito.when(span2.hasEnded()).thenReturn(false);
        Mockito.when(span1.getParentSpan()).thenReturn(span2);
        Mockito.when(span2.getParentSpan()).thenReturn(null);

        TraceEventsRunnable traceEventsRunnable = new TraceEventsRunnable(delegate, traceEventsService);

        traceEventsRunnable.run();

        Mockito.verify(traceEventListener1, Mockito.times(2)).onRunnableStart(Mockito.any(Span.class), Mockito.any(Thread.class));
        Mockito.verify(traceEventListener2, Mockito.times(2)).onRunnableStart(Mockito.any(Span.class), Mockito.any(Thread.class));
        Mockito.verify(traceEventListener1, Mockito.times(2)).onRunnableComplete(Mockito.any(Span.class), Mockito.any(Thread.class));
        Mockito.verify(traceEventListener2, Mockito.times(2)).onRunnableComplete(Mockito.any(Span.class), Mockito.any(Thread.class));

        // Ensure that delegate.run() was invoked
        Mockito.verify(delegate).run();
    }

    public void testRun_TracingNotEnabled_NoInteractionsWithListeners() {
        Mockito.when(traceEventsService.isTracingEnabled()).thenReturn(false);

        TraceEventsRunnable traceEventsRunnable = new TraceEventsRunnable(delegate, traceEventsService);

        traceEventsRunnable.run();

        // Verify that no interactions with listeners occurred
        Mockito.verifyNoInteractions(traceEventListener1);
        Mockito.verifyNoInteractions(traceEventListener2);
    }

    public void testRun_ExceptionInOnRunnableStart_NoImpactOnExecution() {
        Mockito.doThrow(new RuntimeException("Listener 1 exception"))
            .when(traceEventListener1)
            .onRunnableStart(Mockito.eq(span), Mockito.eq(currentThread));
        TraceEventsRunnable traceEventsRunnable = new TraceEventsRunnable(delegate, traceEventsService);
        traceEventsRunnable.run();

        // Ensure that delegate.run() was invoked
        Mockito.verify(delegate).run();
    }

    public void testRun_ExceptionInOnRunnableComplete_NoImpactOnExecution() {
        // trace event listener to throw an exception in onRunnableComplete
        Mockito.doThrow(new RuntimeException("Listener 1 exception"))
            .when(traceEventListener1)
            .onRunnableComplete(Mockito.eq(span), Mockito.eq(currentThread));
        TraceEventsRunnable traceEventsRunnable = new TraceEventsRunnable(delegate, traceEventsService);
        traceEventsRunnable.run();

        // Verify that onRunnableStart was called for the listener despite the exception
        Mockito.verify(traceEventListener1).onRunnableStart(Mockito.any(Span.class), Mockito.any(Thread.class));
        Mockito.verify(delegate).run();
    }

    public void testUnwrap() {
        TraceEventsRunnable traceEventsRunnable = new TraceEventsRunnable(delegate, traceEventsService);

        Runnable unwrappedRunnable = traceEventsRunnable.unwrap();
        assertSame(delegate, unwrappedRunnable);
    }
}
