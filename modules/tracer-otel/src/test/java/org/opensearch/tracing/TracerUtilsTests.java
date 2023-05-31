/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tracing.noop.NoopSpan;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.tracing.DefaultTracer.CURRENT_SPAN;

public class TracerUtilsTests extends OpenSearchTestCase {

    private static final String TRACE_ID = "4aa59968f31dcbff7807741afa9d7d62";
    private static final String SPAN_ID = "bea205cd25756b5e";

    public void testAddTracerContextToHeader() {
        Span mockSpan = mock(Span.class);
        when(mockSpan.getSpanContext()).thenReturn(SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault()));
        OTelSpan span = new OTelSpan("spanName", mockSpan, null, Level.INFO);
        SpanHolder spanHolder = new SpanHolder(span);
        Map<String, Object> transientHeaders = Map.of(CURRENT_SPAN, spanHolder);
        Map<String, String> requestHeaders = new HashMap<>();
        TracerUtils.addTracerContextToHeader().accept(requestHeaders, transientHeaders);
        assertEquals("00-" + TRACE_ID + "-" + SPAN_ID + "-00", requestHeaders.get("traceparent"));
    }

    public void testAddTracerContextToHeaderWithNoopSpan() {
        Span mockSpan = mock(Span.class);
        when(mockSpan.getSpanContext()).thenReturn(SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault()));
        OTelSpan span = new OTelSpan("spanName", mockSpan, null, Level.INFO);
        NoopSpan noopSpan = new NoopSpan("noopSpanName", span, Level.INFO);
        SpanHolder spanHolder = new SpanHolder(noopSpan);
        Map<String, Object> transientHeaders = Map.of(CURRENT_SPAN, spanHolder);
        Map<String, String> requestHeaders = new HashMap<>();
        TracerUtils.addTracerContextToHeader().accept(requestHeaders, transientHeaders);
        assertEquals("00-" + TRACE_ID + "-" + SPAN_ID + "-00", requestHeaders.get("traceparent"));
    }

    public void testExtractTracerContextFromHeader() {
        Map<String, String> requestHeaders = new HashMap<>();
        requestHeaders.put("traceparent", "00-" + TRACE_ID + "-" + SPAN_ID + "-00");
        Context context = TracerUtils.extractTracerContextFromHeader(requestHeaders);
        assertEquals(TRACE_ID, Span.fromContext(context).getSpanContext().getTraceId());
        assertEquals(SPAN_ID, Span.fromContext(context).getSpanContext().getSpanId());
    }
}
