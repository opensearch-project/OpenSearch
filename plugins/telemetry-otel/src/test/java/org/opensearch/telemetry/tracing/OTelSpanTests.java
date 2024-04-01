/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.attributes.SamplingAttributes;
import org.opensearch.test.OpenSearchTestCase;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.sdk.trace.ReadWriteSpan;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

    public void testGetSpanBoolean() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        assertNull(oTelSpan.getAttributeBoolean("key"));
    }

    public void testGetSpanString() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        assertNull(oTelSpan.getAttributeString("key"));
    }

    public void testGetSpanLong() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        assertNull(oTelSpan.getAttributeLong("key"));
    }

    public void testGetSpanDouble() {
        Span mockSpan = getMockSpan();
        OTelSpan oTelSpan = new OTelSpan("spanName", mockSpan, null);
        assertNull(oTelSpan.getAttributeDouble("key"));
    }

    public void testSpanOutlier() {
        ReadWriteSpan mockReadWriteSpan = mock(ReadWriteSpan.class);
        Span mockSpan = getMockSpan();
        OTelSpan mockParent = new OTelSpan("parentSpan", mockSpan, null);
        OTelSpan oTelSpan = new OTelSpan("spanName", mockReadWriteSpan, mockParent);
        when(mockReadWriteSpan.getAttribute(AttributeKey.booleanKey(SamplingAttributes.SAMPLED.getValue()))).thenReturn(true);
        when(mockReadWriteSpan.getAttribute(AttributeKey.stringKey(SamplingAttributes.SAMPLER.getValue()))).thenReturn(
            SamplingAttributes.INFERRED_SAMPLER.getValue()
        );
        oTelSpan.endSpan();
        verify(mockSpan).setAttribute(SamplingAttributes.SAMPLED.getValue(), true);
    }

    public void testSpanAttributes() {
        ReadWriteSpan mockReadWriteSpan = mock(ReadWriteSpan.class);
        OTelSpan oTelSpan = new OTelSpan("spanName", mockReadWriteSpan, null);
        oTelSpan.addAttribute("key", 0.0);
        when(mockReadWriteSpan.getAttribute(AttributeKey.doubleKey("key"))).thenReturn(0.0);
        verify(mockReadWriteSpan).setAttribute("key", 0.0);
        assertEquals(0.0, (Object) oTelSpan.getAttributeDouble("key"));

        oTelSpan.addAttribute("key1", 0L);
        when(mockReadWriteSpan.getAttribute(AttributeKey.longKey("key1"))).thenReturn(0L);
        verify(mockReadWriteSpan).setAttribute("key1", 0L);
        assertEquals(0L, (Object) oTelSpan.getAttributeLong("key1"));
    }

    private Span getMockSpan() {
        Span mockSpan = mock(Span.class);
        when(mockSpan.getSpanContext()).thenReturn(SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault()));
        return mockSpan;
    }
}
