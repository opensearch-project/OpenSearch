/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OTelSpanTests extends OpenSearchTestCase {

    private static final String TRACE_ID = "4aa59968f31dcbff7807741afa9d7d62";
    private static final String SPAN_ID = "bea205cd25756b5e";

    public void testEndSpanTest() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        oTelSpan.endSpan();
        verify(mockSpan).end();
    }

    public void testAddAttributeString() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        oTelSpan.addAttribute("key", "value");

        verify(mockSpan).setAttribute("key", "value");
    }

    public void testAddAttributeLong() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        oTelSpan.addAttribute("key", 1L);

        verify(mockSpan).setAttribute("key", 1L);
    }

    public void testAddAttributeDouble() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        oTelSpan.addAttribute("key", 1.0);

        verify(mockSpan).setAttribute("key", 1.0);
    }

    public void testAddAttributeBoolean() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        oTelSpan.addAttribute("key", true);

        verify(mockSpan).setAttribute("key", true);
    }

    public void testAddEvent() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        oTelSpan.addEvent("eventName");

        verify(mockSpan).addEvent("eventName");
    }

    public void testGetTraceId() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);

        assertEquals(TRACE_ID, oTelSpan.getTraceId());
    }

    public void testGetSpanId() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);

        assertEquals(SPAN_ID, oTelSpan.getSpanId());
    }

    private Span getMockSpan() {
        Span mockSpan = mock(Span.class);
        when(mockSpan.getSpanContext()).thenReturn(SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault()));
        return mockSpan;
    }
}
