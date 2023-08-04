/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listeners;

import org.opensearch.telemetry.diagnostics.DiagnosticSpan;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.TracerWrapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;

public class TraceEventsServiceTests extends OpenSearchTestCase {

    private Tracer tracer;
    private TraceEventsService traceEventsService;
    private TraceEventListener traceEventListener1;
    private TraceEventListener traceEventListener2;
    private Span span;
    private Thread thread;
    private Runnable runnable;
    private DiagnosticSpan diagnosticSpan;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        tracer = mock(Tracer.class);
        traceEventsService = new TraceEventsService();
        traceEventListener1 = mock(TraceEventListener.class);
        traceEventListener2 = mock(TraceEventListener.class);
        span = mock(Span.class);
        thread = mock(Thread.class);
        runnable = mock(Runnable.class);
        diagnosticSpan = mock(DiagnosticSpan.class);

        traceEventsService.registerTraceEventListener("listener1", traceEventListener1);
        traceEventsService.registerTraceEventListener("listener2", traceEventListener2);
    }

    public void testRegisterAndDeregisterTraceEventListener() {
        Map<String, TraceEventListener> traceEventListeners = traceEventsService.getTraceEventListeners();
        assertEquals(2, traceEventListeners.size());

        // Deregister traceEventListener1
        traceEventsService.deregisterTraceEventListener("listener1");
        traceEventListeners = traceEventsService.getTraceEventListeners();
        assertEquals(1, traceEventListeners.size());
        assertNull(traceEventListeners.get("listener1"));
        assertNotNull(traceEventListeners.get("listener2"));
    }

    public void testWrapRunnable() {
        // Tracing is not enabled, the original runnable should be returned
        traceEventsService.setTracingEnabled(false);
        Runnable wrappedRunnable = traceEventsService.wrapRunnable(runnable);
        assertSame(runnable, wrappedRunnable);

        // Tracing is enabled, the wrapped TraceEventsRunnable should be returned
        traceEventsService.setTracingEnabled(true);
        wrappedRunnable = traceEventsService.wrapRunnable(runnable);
        assertTrue(wrappedRunnable instanceof TraceEventsRunnable);
    }

    public void testUnwrapRunnable() {
        // Runnable is not wrapped, should return the same runnable
        traceEventsService.setTracingEnabled(false);
        Runnable unwrappedRunnable = traceEventsService.unwrapRunnable(runnable);
        assertSame(runnable, unwrappedRunnable);

        // Runnable is wrapped, should return the original runnable from TraceEventsRunnable
        traceEventsService.setTracingEnabled(true);
        Runnable wrappedRunnable = traceEventsService.wrapRunnable(runnable);
        unwrappedRunnable = traceEventsService.unwrapRunnable(wrappedRunnable);
        assertSame(runnable, unwrappedRunnable);
    }

    public void testWrapAndSetTracer() {
        traceEventsService.setTracingEnabled(true);
        traceEventsService.wrapAndSetTracer(tracer);
        assertTrue(traceEventsService.getTracer() instanceof TracerWrapper);
    }

    public void testUnwrapTracer() {
        traceEventsService.setTracingEnabled(true);
        TracerWrapper wrappedTracer = traceEventsService.wrapAndSetTracer(tracer);
        Tracer unwrappedTracer = traceEventsService.unwrapTracer(wrappedTracer);
        assertSame(tracer, unwrappedTracer);
    }

    public void testExecuteListeners() {
        when(traceEventListener1.isEnabled(any(Span.class))).thenReturn(true);
        when(traceEventListener2.isEnabled(any(Span.class))).thenReturn(false);

        traceEventsService.setTracingEnabled(true);
        traceEventsService.setDiagnosisEnabled(true);

        traceEventsService.executeListeners(span, traceEventListener -> traceEventListener.onRunnableStart(span, thread));
        traceEventsService.executeListeners(span, traceEventListener -> traceEventListener.onRunnableComplete(span, thread));

        // Verify listener1 is invoked once for onRunnableStart and onRunnableComplete
        verify(traceEventListener1).onRunnableStart(eq(span), eq(thread));
        verify(traceEventListener1).onRunnableComplete(eq(span), eq(thread));

        // Verify listener2 is not invoked
        verify(traceEventListener2, never()).onRunnableStart(any(Span.class), any(Thread.class));
        verify(traceEventListener2, never()).onRunnableComplete(any(Span.class), any(Thread.class));
    }

    public void testExecuteListeners_ExceptionInListener() {
        doThrow(new RuntimeException("Listener 1 exception")).when(traceEventListener1).isEnabled(any());

        traceEventsService.setTracingEnabled(true);
        traceEventsService.setDiagnosisEnabled(true);

        traceEventsService.executeListeners(span, traceEventListener -> traceEventListener.isEnabled(span));

        // Verify that both listeners were called despite the exception in listener1
        verify(traceEventListener1).isEnabled(span);
        verify(traceEventListener2).isEnabled(span);
    }
}
