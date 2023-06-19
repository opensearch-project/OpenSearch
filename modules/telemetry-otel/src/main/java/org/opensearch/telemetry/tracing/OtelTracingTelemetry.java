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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

/**
 * OTel based Telemetry provider
 */
public class OtelTracingTelemetry implements TracingTelemetry {

    private static final Logger logger = LogManager.getLogger(OtelTracingTelemetry.class);

    private final OpenTelemetry openTelemetry;
    private final io.opentelemetry.api.trace.Tracer otelTracer;

    /**
     * Creates OTel based Telemetry
     * @param openTelemetry OpenTelemetry instance
     */
    public OtelTracingTelemetry(OpenTelemetry openTelemetry) {
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
    public Span createSpan(String spanName, Span parentSpan, Level level) {
        return createOtelSpan(spanName, parentSpan, level);
    }

    @Override
    public TracingContextPropagator getContextPropagator() {
        return new OtelTracingContextPropagator(openTelemetry);
    }

    private Span createOtelSpan(String spanName, Span parentSpan, Level level) {
        io.opentelemetry.api.trace.Span otelSpan = createOtelSpan(spanName, parentSpan);
        return new OTelSpan(spanName, otelSpan, parentSpan, level);
    }

    io.opentelemetry.api.trace.Span createOtelSpan(String spanName, Span parentOTelSpan) {
        return parentOTelSpan == null || !(parentOTelSpan instanceof OTelSpan)
            ? otelTracer.spanBuilder(spanName).startSpan()
            : otelTracer.spanBuilder(spanName).setParent(Context.current().with(((OTelSpan) parentOTelSpan).getOtelSpan())).startSpan();
    }
}
