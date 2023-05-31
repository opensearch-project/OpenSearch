/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Default implementation of {@link Span} using Otel span. It keeps a reference of OpenTelemetry Span and handles span
 * lifecycle management by delegating calls to it.
 */
class OTelSpan extends AbstractSpan {

    private static final Logger logger = LogManager.getLogger(OTelSpan.class);

    private final io.opentelemetry.api.trace.Span otelSpan;

    public OTelSpan(String spanName, io.opentelemetry.api.trace.Span span, Span parentSpan, Level level) {
        super(spanName, parentSpan, level);
        this.otelSpan = span;
        logger.trace(
            "Starting OtelSpan spanId:{} name:{}: traceId:{}",
            otelSpan.getSpanContext().getSpanId(),
            spanName,
            otelSpan.getSpanContext().getTraceId()
        );
    }

    @Override
    public void endSpan() {
        logger.trace(
            "Ending span spanId:{} name:{}: traceId:{}",
            otelSpan.getSpanContext().getSpanId(),
            spanName,
            otelSpan.getSpanContext().getTraceId()
        );
        otelSpan.end();
    }

    @Override
    public void addAttribute(String key, String value) {
        otelSpan.setAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Long value) {
        otelSpan.setAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Double value) {
        otelSpan.setAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Boolean value) {
        otelSpan.setAttribute(key, value);
    }

    @Override
    public void addEvent(String event) {
        otelSpan.addEvent(event);
    }

    @Override
    public String getTraceId() {
        return otelSpan.getSpanContext().getTraceId();
    }

    @Override
    public String getSpanId() {
        return otelSpan.getSpanContext().getSpanId();
    }

    io.opentelemetry.api.trace.Span getOtelSpan() {
        return otelSpan;
    }

}
