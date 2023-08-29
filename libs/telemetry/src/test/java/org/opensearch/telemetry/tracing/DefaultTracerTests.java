/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.telemetry.tracing.MockSpan;
import org.opensearch.test.telemetry.tracing.MockTracingTelemetry;
import org.junit.Assert;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        Assert.assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
    }

    public void testCreateSpanWithAttributesWithMock() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        when(mockTracingTelemetry.createSpan("span_name", mockParentSpan, attributes)).thenReturn(mockSpan);
        defaultTracer.startSpan("span_name", attributes);
        verify(mockTracingTelemetry).createSpan("span_name", mockParentSpan, attributes);
    }

    public void testCreateSpanWithAttributesWithParentMock() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        when(mockTracingTelemetry.createSpan("span_name", mockParentSpan, attributes)).thenReturn(mockSpan);
        defaultTracer.startSpan("span_name", new SpanContext(mockParentSpan), attributes);
        verify(mockTracingTelemetry).createSpan("span_name", mockParentSpan, attributes);
        verify(mockTracerContextStorage, never()).get(TracerContextStorage.CURRENT_SPAN);
    }

    public void testCreateSpanWithAttributes() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextBasedTracerContextStorage(new ThreadContext(Settings.EMPTY), tracingTelemetry)
        );

        defaultTracer.startSpan(
            "span_name",
            Attributes.create().addAttribute("key1", 1.0).addAttribute("key2", 2l).addAttribute("key3", true).addAttribute("key4", "key4")
        );

        Assert.assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        Assert.assertEquals(1.0, ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key1"));
        Assert.assertEquals(2l, ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key2"));
        Assert.assertEquals(true, ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key3"));
        Assert.assertEquals("key4", ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key4"));
    }

    public void testCreateSpanWithParent() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextBasedTracerContextStorage(new ThreadContext(Settings.EMPTY), tracingTelemetry)
        );

        defaultTracer.startSpan("span_name", null);

        SpanContext parentSpan = defaultTracer.getCurrentSpan();

        defaultTracer.startSpan("span_name_1", parentSpan, Attributes.EMPTY);

        Assert.assertEquals("span_name_1", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        Assert.assertEquals(parentSpan.getSpan(), defaultTracer.getCurrentSpan().getSpan().getParentSpan());
    }

    public void testCreateSpanWithNullParent() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextBasedTracerContextStorage(new ThreadContext(Settings.EMPTY), tracingTelemetry)
        );

        defaultTracer.startSpan("span_name", null, Attributes.EMPTY);

        Assert.assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        Assert.assertEquals(null, defaultTracer.getCurrentSpan().getSpan().getParentSpan());
    }

    public void testEndSpanByClosingScope() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        try (SpanScope spanScope = defaultTracer.startSpan("span_name")) {
            verify(mockTracerContextStorage).put(TracerContextStorage.CURRENT_SPAN, mockSpan);
        }
        verify(mockTracerContextStorage).put(TracerContextStorage.CURRENT_SPAN, mockParentSpan);
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
        when(mockTracingTelemetry.createSpan(eq("span_name"), eq(mockParentSpan), any(Attributes.class))).thenReturn(mockSpan);
    }
}
