/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.junit.Assert;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.opensearch.telemetry.tracing.DefaultTracer.CURRENT_SPAN;

public class DefaultTracerTests extends OpenSearchTestCase {

    private TracingTelemetry mockTracingTelemetry;
    private TracerContextStorage<String, Span> mockTracerContextStorage;
    private Span mockSpan;
    private Span mockParentSpan;
    private Supplier<Level> levelSupplier;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setupMocks();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testCreateSpan() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage, levelSupplier);

        defaultTracer.startSpan("span_name", Level.INFO);

        Assert.assertEquals("span_name", defaultTracer.getCurrentSpan().getSpanName());
    }

    public void testEndSpan() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage, levelSupplier);
        defaultTracer.startSpan("span_name", Level.INFO);
        verify(mockTracerContextStorage).put(CURRENT_SPAN, mockSpan);

        defaultTracer.endSpan();
        verify(mockSpan).endSpan();
        verify(mockTracerContextStorage).put(CURRENT_SPAN, mockParentSpan);
    }

    public void testEndSpanByClosingScope() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage, levelSupplier);
        try (Scope scope = defaultTracer.startSpan("span_name", Level.INFO)) {
            verify(mockTracerContextStorage).put(CURRENT_SPAN, mockSpan);
        }
        verify(mockTracerContextStorage).put(CURRENT_SPAN, mockParentSpan);
    }

    public void testAddSpanAttributeString() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage, levelSupplier);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanAttribute("key", "value");

        verify(mockSpan).addAttribute("key", "value");
    }

    public void testAddSpanAttributeLong() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage, levelSupplier);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanAttribute("key", 1L);

        verify(mockSpan).addAttribute("key", 1L);
    }

    public void testAddSpanAttributeDouble() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage, levelSupplier);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanAttribute("key", 1.0);

        verify(mockSpan).addAttribute("key", 1.0);
    }

    public void testAddSpanAttributeBoolean() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage, levelSupplier);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanAttribute("key", true);

        verify(mockSpan).addAttribute("key", true);
    }

    public void testAddEvent() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage, levelSupplier);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanEvent("eventName");

        verify(mockSpan).addEvent("eventName");
    }

    public void testClose() throws IOException {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage, levelSupplier);

        defaultTracer.close();

        verify(mockTracingTelemetry).close();
    }

    @SuppressWarnings("unchecked")
    private void setupMocks() {
        levelSupplier = () -> Level.INFO;
        mockTracingTelemetry = mock(TracingTelemetry.class);
        mockSpan = mock(Span.class);
        mockParentSpan = mock(Span.class);
        mockTracerContextStorage = mock(TracerContextStorage.class);
        when(mockSpan.getSpanName()).thenReturn("span_name");
        when(mockSpan.getSpanId()).thenReturn("span_id");
        when(mockSpan.getTraceId()).thenReturn("trace_id");
        when(mockSpan.getLevel()).thenReturn(Level.INFO);
        when(mockSpan.getParentSpan()).thenReturn(mockParentSpan);
        when(mockParentSpan.getSpanId()).thenReturn("parent_span_id");
        when(mockParentSpan.getTraceId()).thenReturn("trace_id");
        when(mockParentSpan.getLevel()).thenReturn(Level.INFO);
        when(mockTracerContextStorage.get(CURRENT_SPAN)).thenReturn(mockParentSpan, mockSpan);
        when(mockTracingTelemetry.createSpan("span_name", mockParentSpan, Level.INFO)).thenReturn(mockSpan);
    }
}
