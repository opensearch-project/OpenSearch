/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import org.opensearch.test.OpenSearchTestCase;

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
        when(mockSpanBuilder.startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));

        TracingTelemetry tracingTelemetry = new OTelTracingTelemetry(mockOpenTelemetry);
        Span span = tracingTelemetry.createSpan("span_name", null);

        verify(mockSpanBuilder, never()).setParent(any());
        assertNull(span.getParentSpan());
    }

    public void testCreateSpanWithParent() {
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        Tracer mockTracer = mock(Tracer.class);
        when(mockOpenTelemetry.getTracer("os-tracer")).thenReturn(mockTracer);
        SpanBuilder mockSpanBuilder = mock(SpanBuilder.class);
        when(mockTracer.spanBuilder("span_name")).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.setParent(any())).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));

        Span parentSpan = new OTelSpan("parent_span", mock(io.opentelemetry.api.trace.Span.class), null);

        TracingTelemetry tracingTelemetry = new OTelTracingTelemetry(mockOpenTelemetry);
        Span span = tracingTelemetry.createSpan("span_name", parentSpan);

        verify(mockSpanBuilder).setParent(any());
        assertNotNull(span.getParentSpan());
        assertEquals("parent_span", span.getParentSpan().getSpanName());
    }

    public void testGetContextPropagator() {
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        Tracer mockTracer = mock(Tracer.class);
        when(mockOpenTelemetry.getTracer("os-tracer")).thenReturn(mockTracer);

        TracingTelemetry tracingTelemetry = new OTelTracingTelemetry(mockOpenTelemetry);

        assertTrue(tracingTelemetry.getContextPropagator() instanceof OTelTracingContextPropagator);
    }

}
