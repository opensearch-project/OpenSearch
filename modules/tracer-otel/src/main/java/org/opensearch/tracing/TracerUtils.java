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
import org.opensearch.tracing.noop.NoopSpan;

import java.util.Map;
import java.util.function.BiConsumer;

import static org.opensearch.tracing.DefaultTracer.CURRENT_SPAN;

/**
 * Contains utils methods for tracing
 */
public class TracerUtils {

    private TracerUtils() {

    }

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
    public static final BiConsumer<Map<String, String>, Map<String, Object>> addTracerContextToHeader() {
        return (requestHeaders, transientHeaders) -> {
            if (transientHeaders != null && transientHeaders.containsKey(CURRENT_SPAN)) {
                SpanHolder spanHolder = (SpanHolder) transientHeaders.get(CURRENT_SPAN);
                Span currentSpan = spanHolder.getSpan();
                OTelSpan oTelSpan = getLastValidSpanInChain(currentSpan);
                OTelResourceProvider.getContextPropagators()
                    .getTextMapPropagator()
                    .inject(context(oTelSpan), requestHeaders, TEXT_MAP_SETTER);
            }
        };
    }

    /**
     * Fetches the tracing context from headers
     * @param headers request header map
     */
    public static Context extractTracerContextFromHeader(Map<String, String> headers) {
        return OTelResourceProvider.getContextPropagators().getTextMapPropagator().extract(Context.current(), headers, TEXT_MAP_GETTER);
    }

    private static Context context(OTelSpan oTelSpan) {
        return Context.current().with(io.opentelemetry.api.trace.Span.wrap(oTelSpan.getOtelSpan().getSpanContext()));
    }

    private static OTelSpan getLastValidSpanInChain(Span span) {
        while (span instanceof NoopSpan) {
            span = span.getParentSpan();
        }
        return (OTelSpan) span;
    }
}
