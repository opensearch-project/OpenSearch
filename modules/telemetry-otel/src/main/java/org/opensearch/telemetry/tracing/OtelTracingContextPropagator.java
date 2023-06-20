/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.opensearch.telemetry.tracing.DefaultTracer.CURRENT_SPAN;

/**
 * Otel implementation of TracingContextPropagator
 */
public class OtelTracingContextPropagator implements TracingContextPropagator {

    private final OpenTelemetry openTelemetry;

    /**
     * Creates OtelTracingContextPropagator instance
     * @param openTelemetry Otel OpenTelemetry instance
     */
    public OtelTracingContextPropagator(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
    }

    @Override
    public Span extract(Map<String, String> props) {
        Context context = openTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(), props, TEXT_MAP_GETTER);
        if (context != null) {
            io.opentelemetry.api.trace.Span span = io.opentelemetry.api.trace.Span.fromContext(context);
            return new PropagatedSpan(span);
        }
        return null;
    }

    @Override
    public BiConsumer<Map<String, String>, Map<String, Object>> inject() {
        return (requestHeaders, transientHeaders) -> {
            if (transientHeaders != null && transientHeaders.containsKey(CURRENT_SPAN)) {
                if (transientHeaders.get(CURRENT_SPAN) instanceof AtomicReference) {
                    @SuppressWarnings("unchecked")
                    AtomicReference<Span> currentSpanRef = (AtomicReference<Span>) transientHeaders.get(CURRENT_SPAN);
                    Span currentSpan = currentSpanRef.get();
                    if (currentSpan instanceof OTelSpan) {
                        openTelemetry.getPropagators()
                            .getTextMapPropagator()
                            .inject(context((OTelSpan) currentSpan), requestHeaders, TEXT_MAP_SETTER);
                    }
                }
            }
        };
    }

    private static Context context(OTelSpan oTelSpan) {
        return Context.current().with(io.opentelemetry.api.trace.Span.wrap(oTelSpan.getOtelSpan().getSpanContext()));
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

}
