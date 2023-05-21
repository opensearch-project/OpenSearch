/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import java.util.Map;

import static org.opensearch.tracing.DefaultTracer.CURRENT_SPAN;

/**
 * Contains utils methods for tracing
 */
public class TracerUtils {

    private static final TextMapSetter<Map<String, String>> TEXT_MAP_SETTER = (carrier, key, value) -> {
        if (carrier != null) {
            carrier.put(key, value);
        }
    };

    private static final TextMapGetter<Map<String, String>> TEXT_MAP_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> headers) {
            return headers.keySet();
        }

        @Override
        public String get(Map<String, String> headers, String key) {
            if (headers != null && headers.containsKey(key)) {
                return headers.get(key);
            }
            return null;
        }
    };

    /**
     * Adds current active span as tracing context in the header during outbound calls
     */
    public static void addTracerContextToHeader(Map<String, String> requestHeaders, Map<String, Object> transientHeaders) {
        if (transientHeaders != null && transientHeaders.containsKey(CURRENT_SPAN)) {
            SpanHolder spanHolder = (SpanHolder) transientHeaders.get(CURRENT_SPAN);
            Span currentSpan = spanHolder.getSpan();
            OSSpan osSpan = getLastValidSpanInChain(currentSpan);
            OTelResourceProvider.getContextPropagators().getTextMapPropagator().inject(context(osSpan), requestHeaders, TEXT_MAP_SETTER);
        }
    }

    /**
     * Fetches the tracing context from headers
     */
    public static Context extractTracerContextFromHeader(Map<String, String> headers) {
        return OTelResourceProvider.getContextPropagators().getTextMapPropagator().extract(Context.current(), headers, TEXT_MAP_GETTER);
    }

    private static Context context(OSSpan osSpan) {
        return Context.current().with(io.opentelemetry.api.trace.Span.wrap(osSpan.getSpanContext()));
    }

    private static OSSpan getLastValidSpanInChain(Span span) {
        while (span instanceof NoopSpan) {
            span = span.getParentSpan();
        }
        return (OSSpan) span;
    }
}
