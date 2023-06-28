/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OTelTracingContextPropagatorTests extends OpenSearchTestCase {

    private static final String TRACE_ID = "4aa59968f31dcbff7807741afa9d7d62";
    private static final String SPAN_ID = "bea205cd25756b5e";

    public void testAddTracerContextToHeader() {
        Span mockSpan = mock(Span.class);
        when(mockSpan.getSpanContext()).thenReturn(SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault()));
        OTelSpan span = new OTelSpan("spanName", mockSpan, null);
        Map<String, String> requestHeaders = new HashMap<>();
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        when(mockOpenTelemetry.getPropagators()).thenReturn(ContextPropagators.create(W3CTraceContextPropagator.getInstance()));
        TracingContextPropagator tracingContextPropagator = new OTelTracingContextPropagator(mockOpenTelemetry);

        tracingContextPropagator.inject(span, (key, value) -> requestHeaders.put(key, value));
        assertEquals("00-" + TRACE_ID + "-" + SPAN_ID + "-00", requestHeaders.get("traceparent"));
    }

    public void testExtractTracerContextFromHeader() {
        Map<String, String> requestHeaders = new HashMap<>();
        requestHeaders.put("traceparent", "00-" + TRACE_ID + "-" + SPAN_ID + "-00");
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        when(mockOpenTelemetry.getPropagators()).thenReturn(ContextPropagators.create(W3CTraceContextPropagator.getInstance()));
        TracingContextPropagator tracingContextPropagator = new OTelTracingContextPropagator(mockOpenTelemetry);
        org.opensearch.telemetry.tracing.Span span = tracingContextPropagator.extract(requestHeaders);
        assertEquals(TRACE_ID, span.getTraceId());
        assertEquals(SPAN_ID, span.getSpanId());
    }
}
