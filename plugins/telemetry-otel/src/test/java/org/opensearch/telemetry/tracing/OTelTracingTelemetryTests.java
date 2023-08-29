/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OTelTracingTelemetryTests extends OpenSearchTestCase {

    public void testCreateSpanWithoutParent() {
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        Tracer mockTracer = mock(Tracer.class);
        when(mockOpenTelemetry.getTracer("os-tracer")).thenReturn(mockTracer);
        SpanBuilder mockSpanBuilder = mock(SpanBuilder.class);
        when(mockTracer.spanBuilder("span_name")).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.setAllAttributes(any(io.opentelemetry.api.common.Attributes.class))).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));
        Map<String, String> attributeMap = Collections.singletonMap("name", "value");
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        TracingTelemetry tracingTelemetry = new OTelTracingTelemetry(mockOpenTelemetry);
        Span span = tracingTelemetry.createSpan("span_name", null, attributes);

        verify(mockSpanBuilder, never()).setParent(any());
        verify(mockSpanBuilder).setAllAttributes(createAttribute(attributes));
        assertNull(span.getParentSpan());
    }

    public void testCreateSpanWithParent() {
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        Tracer mockTracer = mock(Tracer.class);
        when(mockOpenTelemetry.getTracer("os-tracer")).thenReturn(mockTracer);
        SpanBuilder mockSpanBuilder = mock(SpanBuilder.class);
        when(mockTracer.spanBuilder("span_name")).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.setParent(any())).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.setAllAttributes(any(io.opentelemetry.api.common.Attributes.class))).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));

        Span parentSpan = new OTelSpan("parent_span", mock(io.opentelemetry.api.trace.Span.class), null);

        TracingTelemetry tracingTelemetry = new OTelTracingTelemetry(mockOpenTelemetry);
        Attributes attributes = Attributes.create().addAttribute("name", 1l);
        Span span = tracingTelemetry.createSpan("span_name", parentSpan, attributes);

        verify(mockSpanBuilder).setParent(any());
        verify(mockSpanBuilder).setAllAttributes(createAttributeLong(attributes));
        assertNotNull(span.getParentSpan());

        assertEquals("parent_span", span.getParentSpan().getSpanName());
    }

    public void testCreateSpanWithParentWithMultipleAttributes() {
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        Tracer mockTracer = mock(Tracer.class);
        when(mockOpenTelemetry.getTracer("os-tracer")).thenReturn(mockTracer);
        SpanBuilder mockSpanBuilder = mock(SpanBuilder.class);
        when(mockTracer.spanBuilder("span_name")).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.setParent(any())).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.setAllAttributes(any(io.opentelemetry.api.common.Attributes.class))).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));

        Span parentSpan = new OTelSpan("parent_span", mock(io.opentelemetry.api.trace.Span.class), null);

        TracingTelemetry tracingTelemetry = new OTelTracingTelemetry(mockOpenTelemetry);
        Attributes attributes = Attributes.create()
            .addAttribute("key1", 1l)
            .addAttribute("key2", 2.0)
            .addAttribute("key3", true)
            .addAttribute("key4", "key4");
        Span span = tracingTelemetry.createSpan("span_name", parentSpan, attributes);

        io.opentelemetry.api.common.Attributes otelAttributes = io.opentelemetry.api.common.Attributes.builder()
            .put("key1", 1l)
            .put("key2", 2.0)
            .put("key3", true)
            .put("key4", "key4")
            .build();
        verify(mockSpanBuilder).setParent(any());
        verify(mockSpanBuilder).setAllAttributes(otelAttributes);
        assertNotNull(span.getParentSpan());

        assertEquals("parent_span", span.getParentSpan().getSpanName());
    }

    private io.opentelemetry.api.common.Attributes createAttribute(Attributes attributes) {
        AttributesBuilder attributesBuilder = io.opentelemetry.api.common.Attributes.builder();
        attributes.getAttributesMap().forEach((x, y) -> attributesBuilder.put(x, (String) y));
        return attributesBuilder.build();
    }

    private io.opentelemetry.api.common.Attributes createAttributeLong(Attributes attributes) {
        AttributesBuilder attributesBuilder = io.opentelemetry.api.common.Attributes.builder();
        attributes.getAttributesMap().forEach((x, y) -> attributesBuilder.put(x, (Long) y));
        return attributesBuilder.build();
    }

    public void testGetContextPropagator() {
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        Tracer mockTracer = mock(Tracer.class);
        when(mockOpenTelemetry.getTracer("os-tracer")).thenReturn(mockTracer);

        TracingTelemetry tracingTelemetry = new OTelTracingTelemetry(mockOpenTelemetry);

        assertTrue(tracingTelemetry.getContextPropagator() instanceof OTelTracingContextPropagator);
    }

}
