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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class DefaultTracerTests extends OpenSearchTestCase {

    private TracingTelemetry mockTracingTelemetry;
    private TracerContextStorage<String, Span> mockTracerContextStorage;
    private Span mockSpan;
    private Span mockParentSpan;

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
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);

        defaultTracer.startSpan("span_name");

        Assert.assertEquals("span_name", defaultTracer.getCurrentSpan().getSpanName());
    }

    public void testEndSpanByClosingScope() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        try (Scope scope = defaultTracer.startSpan("span_name")) {
            verify(mockTracerContextStorage).put(TracerContextStorage.CURRENT_SPAN, mockSpan);
        }
        verify(mockTracerContextStorage).put(TracerContextStorage.CURRENT_SPAN, mockParentSpan);
    }

    public void testAddSpanAttributeString() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        defaultTracer.startSpan("span_name");

        defaultTracer.addSpanAttribute("key", "value");

        verify(mockSpan).addAttribute("key", "value");
    }

    public void testAddSpanAttributeLong() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        defaultTracer.startSpan("span_name");

        defaultTracer.addSpanAttribute("key", 1L);

        verify(mockSpan).addAttribute("key", 1L);
    }

    public void testAddSpanAttributeDouble() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        defaultTracer.startSpan("span_name");

        defaultTracer.addSpanAttribute("key", 1.0);

        verify(mockSpan).addAttribute("key", 1.0);
    }

    public void testAddSpanAttributeBoolean() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        defaultTracer.startSpan("span_name");

        defaultTracer.addSpanAttribute("key", true);

        verify(mockSpan).addAttribute("key", true);
    }

    public void testAddEvent() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        defaultTracer.startSpan("span_name");

        defaultTracer.addSpanEvent("eventName");

        verify(mockSpan).addEvent("eventName");
    }

    public void testClose() throws IOException {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);

        defaultTracer.close();

        verify(mockTracingTelemetry).close();
    }

    @SuppressWarnings("unchecked")
    private void setupMocks() {
        mockTracingTelemetry = mock(TracingTelemetry.class);
        mockSpan = mock(Span.class);
        mockParentSpan = mock(Span.class);
        mockTracerContextStorage = mock(TracerContextStorage.class);
        when(mockSpan.getSpanName()).thenReturn("span_name");
        when(mockSpan.getSpanId()).thenReturn("span_id");
        when(mockSpan.getTraceId()).thenReturn("trace_id");
        when(mockSpan.getParentSpan()).thenReturn(mockParentSpan);
        when(mockParentSpan.getSpanId()).thenReturn("parent_span_id");
        when(mockParentSpan.getTraceId()).thenReturn("trace_id");
        when(mockTracerContextStorage.get(TracerContextStorage.CURRENT_SPAN)).thenReturn(mockParentSpan, mockSpan);
        when(mockTracingTelemetry.createSpan("span_name", mockParentSpan)).thenReturn(mockSpan);
    }
}
