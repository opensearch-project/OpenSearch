/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.core.common.Strings;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * Otel implementation of TracingContextPropagator
 */
public class OTelTracingContextPropagator implements TracingContextPropagator {

    private final OpenTelemetry openTelemetry;

    /**
     * Creates OTelTracingContextPropagator instance
     * @param openTelemetry Otel OpenTelemetry instance
     */
    public OTelTracingContextPropagator(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
    }

    @Override
    public Span extract(Map<String, String> props) {
        Context context = openTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(), props, TEXT_MAP_GETTER);
        return getPropagatedSpan(context);
    }

    private static OTelPropagatedSpan getPropagatedSpan(Context context) {
        if (context != null) {
            io.opentelemetry.api.trace.Span span = io.opentelemetry.api.trace.Span.fromContext(context);
            return new OTelPropagatedSpan(span);
        }
        return null;
    }

    @Override
    public Optional<Span> extractFromHeaders(Map<String, List<String>> header) {
        Context context = openTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(), header, HEADER_TEXT_MAP_GETTER);
        return Optional.ofNullable(getPropagatedSpan(context));
    }

    @Override
    public void inject(Span currentSpan, BiConsumer<String, String> setter) {
        openTelemetry.getPropagators().getTextMapPropagator().inject(context((OTelSpan) currentSpan), setter, TEXT_MAP_SETTER);

    }

    private static Context context(OTelSpan oTelSpan) {
        return Context.current().with(io.opentelemetry.api.trace.Span.wrap(oTelSpan.getDelegateSpan().getSpanContext()));
    }

    private static final TextMapSetter<BiConsumer<String, String>> TEXT_MAP_SETTER = (carrier, key, value) -> {
        if (carrier != null) {
            carrier.accept(key, value);
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

    private static final TextMapGetter<Map<String, List<String>>> HEADER_TEXT_MAP_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, List<String>> header) {
            if (header != null) {
                return header.keySet();
            } else {
                return Collections.emptySet();
            }
        }

        @Override
        public String get(Map<String, List<String>> header, String key) {
            if (header != null && header.containsKey(key)) {
                return Strings.collectionToCommaDelimitedString(header.get(key));
            }
            return null;
        }
    };

}
