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
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.test.telemetry.tracing.validators.AllSpansAreEndedProperly;
import org.opensearch.test.telemetry.tracing.validators.AllSpansHaveUniqueId;

import java.util.Arrays;
import java.util.List;

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
    public Span createSpan(String spanName, Span parentSpan, Attributes attributes) {
        Span span = new MockSpan(spanName, parentSpan, spanProcessor, attributes);
        spanProcessor.onStart(span);
        return span;
    }

    @Override
    public TracingContextPropagator getContextPropagator() {
        return new MockTracingContextPropagator(spanProcessor);
    }

    @Override
    public void close() {
        List<MockSpanData> spanData = ((StrictCheckSpanProcessor) spanProcessor).getFinishedSpanItems();
        if (spanData.size() != 0) {
            TelemetryValidators validators = new TelemetryValidators(
                Arrays.asList(new AllSpansAreEndedProperly(), new AllSpansHaveUniqueId())
            );
            validators.validate(spanData, 1);
        }
    }
}
