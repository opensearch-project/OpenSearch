/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.TracingContextPropagator;
import org.opensearch.telemetry.tracing.TracingTelemetry;

/**
 * Mock {@link TracingTelemetry} implementation for testing.
 */
public class MockTracingTelemetry implements TracingTelemetry {

    private final SpanProcessor spanProcessor = new StrictCheckSpanProcessor();

    /**
     * Base constructor.
     */
    public MockTracingTelemetry() {

    }

    @Override
    public Span createSpan(String spanName, Span parentSpan) {
        Span span = new MockSpan(spanName, parentSpan, spanProcessor);
        spanProcessor.onStart(span);
        return span;
    }

    @Override
    public TracingContextPropagator getContextPropagator() {
        return new MockTracingContextPropagator(spanProcessor);
    }

    @Override
    public void close() {
        ((StrictCheckSpanProcessor) spanProcessor).ensureAllSpansAreClosed();
        ((StrictCheckSpanProcessor) spanProcessor).clear();
    }
}
