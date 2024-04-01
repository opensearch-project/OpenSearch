/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.processor;

import org.opensearch.telemetry.tracing.attributes.SamplingAttributes;
import org.opensearch.test.OpenSearchTestCase;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OTelSpanProcessorTests extends OpenSearchTestCase {

    private OTelSpanProcessor oTelSpanProcessor;
    private MockSpanProcessor mockSpanProcessor;
    private static final String TRACE_ID = "4aa59968f31dcbff7807741afa9d7d62";
    private static final String SPAN_ID = "bea205cd25756b5e";

    private ReadableSpan createEndedSpan() {
        return Mockito.mock(ReadableSpan.class);
    }

    public void testOTelSpanProcessorDelegation() {
        mockSpanProcessor = new MockSpanProcessor();
        oTelSpanProcessor = new OTelSpanProcessor(mockSpanProcessor);
        assertEquals(mockSpanProcessor.forceFlush(), oTelSpanProcessor.forceFlush());
        assertEquals(mockSpanProcessor.shutdown(), oTelSpanProcessor.shutdown());
        assertEquals(mockSpanProcessor.isEndRequired(), oTelSpanProcessor.isEndRequired());
        assertEquals(mockSpanProcessor.isStartRequired(), oTelSpanProcessor.isStartRequired());
    }

    public void testOnEndFunction() {
        SpanProcessor mockProcessor = mock(SpanProcessor.class);
        oTelSpanProcessor = new OTelSpanProcessor(mockProcessor);
        ReadableSpan readableSpan = this.createEndedSpan();
        when(readableSpan.getSpanContext()).thenReturn(
            SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getDefault(), TraceState.getDefault())
        );
        oTelSpanProcessor.onEnd(readableSpan);
        Mockito.verify(mockProcessor, Mockito.times(1)).onEnd(readableSpan);
    }

    public void testOnStartFunction() {
        SpanProcessor mockProcessor = mock(SpanProcessor.class);
        Context spanContext = mock(Context.class);
        oTelSpanProcessor = new OTelSpanProcessor(mockProcessor);
        ReadWriteSpan readWriteSpan = mock(ReadWriteSpan.class);
        oTelSpanProcessor.onStart(spanContext, readWriteSpan);
        Mockito.verify(mockProcessor, Mockito.times(1)).onStart(spanContext, readWriteSpan);
    }

    public void testOnEndFunctionWithInferredAttribute() {
        SpanProcessor mockProcessor = mock(SpanProcessor.class);
        oTelSpanProcessor = new OTelSpanProcessor(mockProcessor);
        ReadableSpan readableSpan = this.createEndedSpan();
        when(readableSpan.getSpanContext()).thenReturn(
            SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getSampled(), TraceState.getDefault())
        );
        when(readableSpan.getAttribute(AttributeKey.stringKey(SamplingAttributes.SAMPLER.getValue()))).thenReturn(
            SamplingAttributes.INFERRED_SAMPLER.getValue()
        );
        oTelSpanProcessor.onEnd(readableSpan);
        Mockito.verify(mockProcessor, Mockito.times(0)).onEnd(readableSpan);
    }

    public void testOnEndFunctionWithInferredAttributeAndSampled() {
        SpanProcessor mockProcessor = mock(SpanProcessor.class);
        oTelSpanProcessor = new OTelSpanProcessor(mockProcessor);
        ReadableSpan readableSpan = this.createEndedSpan();
        when(readableSpan.getSpanContext()).thenReturn(
            SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getSampled(), TraceState.getDefault())
        );
        when(readableSpan.getAttribute(AttributeKey.stringKey(SamplingAttributes.SAMPLER.getValue()))).thenReturn(
            SamplingAttributes.INFERRED_SAMPLER.getValue()
        );
        when(readableSpan.getAttribute(AttributeKey.booleanKey(SamplingAttributes.SAMPLED.getValue()))).thenReturn(true);
        oTelSpanProcessor.onEnd(readableSpan);
        Mockito.verify(mockProcessor, Mockito.times(1)).onEnd(readableSpan);
    }
}
