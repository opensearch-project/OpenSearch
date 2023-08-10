/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import java.util.Locale;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import org.opensearch.telemetry.tracing.attributes.AttributeKey;
import org.opensearch.telemetry.tracing.attributes.Attributes;

/**
 * OTel based Telemetry provider
 */
public class OTelTracingTelemetry implements TracingTelemetry {

    private static final Logger logger = LogManager.getLogger(OTelTracingTelemetry.class);

    private final OpenTelemetry openTelemetry;
    private final io.opentelemetry.api.trace.Tracer otelTracer;

    /**
     * Creates OTel based Telemetry
     * @param openTelemetry OpenTelemetry instance
     */
    public OTelTracingTelemetry(OpenTelemetry openTelemetry) {
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
    public Span createSpan(String spanName, Span parentSpan, Attributes attributes) {
        return createOtelSpan(spanName, parentSpan, attributes);
    }

    @Override
    public TracingContextPropagator getContextPropagator() {
        return new OTelTracingContextPropagator(openTelemetry);
    }

    private Span createOtelSpan(String spanName, Span parentSpan, Attributes attributes) {
        io.opentelemetry.api.trace.Span otelSpan = otelSpan(spanName, parentSpan, convertToAttributes(attributes));
        return new OTelSpan(spanName, otelSpan, parentSpan);
    }

    private io.opentelemetry.api.common.Attributes convertToAttributes(Attributes attributes) {
        if (attributes != null) {
            AttributesBuilder attributesBuilder = io.opentelemetry.api.common.Attributes.builder();
            attributes.getAttributesMap().forEach((x, y) -> addSpanAttribute(x, y, attributesBuilder));
            return attributesBuilder.build();
        } else {
            return io.opentelemetry.api.common.Attributes.builder().build();
        }
    }

    io.opentelemetry.api.trace.Span otelSpan(String spanName, Span parentOTelSpan, io.opentelemetry.api.common.Attributes attributes) {
        return parentOTelSpan == null || !(parentOTelSpan instanceof OTelSpan)
            ? otelTracer.spanBuilder(spanName).setAllAttributes(attributes).startSpan()
            : otelTracer.spanBuilder(spanName)
                .setParent(Context.current().with(((OTelSpan) parentOTelSpan).getDelegateSpan()))
                .setAllAttributes(attributes)
                .startSpan();
    }

    private void addSpanAttribute(AttributeKey key, Object value, AttributesBuilder attributesBuilder) {
        switch (key.getType()) {
            case BOOLEAN:
                attributesBuilder.put(key.getKey(), (Boolean) value);
                break;
            case LONG:
                attributesBuilder.put(key.getKey(), (Long) value);
                break;
            case DOUBLE:
                attributesBuilder.put(key.getKey(), (Double) value);
                break;
            case STRING:
                attributesBuilder.put(key.getKey(), (String) value);
                break;
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Span attribute value %s type not supported", value));
        }
    }
}
