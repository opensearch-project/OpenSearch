/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import org.opensearch.core.common.Strings;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanKind;
import org.opensearch.telemetry.tracing.TracingContextPropagator;
import org.opensearch.telemetry.tracing.attributes.Attributes;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

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
    public Optional<Span> extract(Map<String, String> props) {
        String value = props.get(TRACE_PARENT);
        if (value != null) {
            String[] values = value.split(SEPARATOR);
            String traceId = values[0];
            String spanId = values[1];
            return Optional.of(new MockSpan(null, null, traceId, spanId, spanProcessor, Attributes.EMPTY, SpanKind.INTERNAL));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Span> extractFromHeaders(Map<String, Collection<String>> headers) {
        if (headers != null) {
            Map<String, String> convertedHeader = headers.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> Strings.collectionToCommaDelimitedString(e.getValue())));
            return extract(convertedHeader);
        } else {
            return Optional.empty();
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
