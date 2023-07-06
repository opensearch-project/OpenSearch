/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.TracingContextPropagator;

/**
 * Mock {@link TracingContextPropagator} to persist the span for internode communication.
 */
public class MockTracingContextPropagator implements TracingContextPropagator {

    private static final String TRACE_PARENT = "traceparent";
    private static final String SEPARATOR = "~";
    private final SpanProcessor spanProcessor;

    /**
     * Constructor
     * @param spanProcessor span processor.
     */
    public MockTracingContextPropagator(SpanProcessor spanProcessor) {
        this.spanProcessor = spanProcessor;
    }

    @Override
    public Span extract(Map<String, String> props) {
        String value = props.get(TRACE_PARENT);
        if (value != null) {
            String[] values = value.split(SEPARATOR);
            String traceId = values[0];
            String spanId = values[1];
            return new MockSpan(null, null, traceId, spanId, spanProcessor);
        } else {
            return null;
        }
    }

    @Override
    public void inject(Span currentSpan, BiConsumer<String, String> setter) {
        if (currentSpan instanceof MockSpan) {
            String traceId = currentSpan.getTraceId();
            String spanId = currentSpan.getSpanId();
            String traceParent = String.format(Locale.ROOT, "%s%s%s", traceId, SEPARATOR, spanId);
            setter.accept(TRACE_PARENT, traceParent);
        }
    }
}
