/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class OtelTelemetry implements Telemetry {

    private static final Logger logger = LogManager.getLogger(OtelTelemetry.class);
    private static final String ROOT_SPAN = "root_span";

    private final OpenTelemetry openTelemetry;
    private final io.opentelemetry.api.trace.Tracer otelTracer;

    public OtelTelemetry(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        this.otelTracer = openTelemetry.getTracer("os-tracer");

    }

    @Override
    public void close() {
        try {
            ((Closeable) openTelemetry).close();
        } catch (IOException e) {
            logger.warn("Error while closing Opentelemetry", e);
        }
    }

    @Override
    public Supplier<Span> spanSupplier(String spanName, Span parentSpan, Level level) {
        return  () -> createOtelSpan(spanName, parentSpan, level);
    }

    private Span createOtelSpan(String spanName, Span parentSpan, Level level) {
        io.opentelemetry.api.trace.Span otelSpan = createOtelSpan(spanName, parentSpan);
        return new OTelSpan(spanName, otelSpan, parentSpan, level);
    }

    @Override
    public Supplier<Span> extractSpanFromHeader(Map<String, String> header) {
        Context context = TracerUtils.extractTracerContextFromHeader(header);
        if (context != null) {
            io.opentelemetry.api.trace.Span span = io.opentelemetry.api.trace.Span.fromContext(context);
            return () -> new OTelSpan(ROOT_SPAN, span, null, Level.ROOT);
        }
        return () -> null;
    }

    @Override
    public BiConsumer<Map<String, String>, Map<String, Object>> injectSpanInHeader() {
        return TracerUtils.addTracerContextToHeader();
    }

    io.opentelemetry.api.trace.Span createOtelSpan(String spanName, Span parentOTelSpan) {
        return parentOTelSpan == null
            ? otelTracer.spanBuilder(spanName).startSpan()
            : otelTracer.spanBuilder(spanName).setParent(Context.current().with(((OTelSpan) parentOTelSpan).getOtelSpan())).startSpan();
    }
}
