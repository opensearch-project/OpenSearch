/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.util.Locale;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.TracingContextPropagator;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.telemetry.tracing.attributes.AttributeKey;
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
        Span span = new MockSpan(spanName, parentSpan, spanProcessor);
        if (attributes != null) {
            attributes.getAttributesMap().forEach((x, y) -> addSpanAttribute(x, y, span));
        }
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

    private void addSpanAttribute(AttributeKey key, Object value, Span span) {
        switch (key.getType()) {
            case BOOLEAN:
                span.addAttribute(key.getKey(), (Boolean) value);
                break;
            case LONG:
                span.addAttribute(key.getKey(), (Long) value);
                break;
            case DOUBLE:
                span.addAttribute(key.getKey(), (Double) value);
                break;
            case STRING:
                span.addAttribute(key.getKey(), (String) value);
                break;
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Span attribute value %s type not supported", value));
        }
    }
}
