/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.trace.Span;

/**
 * Default implementation of {@link Span} using Otel span. It keeps a reference of OpenTelemetry Span and handles span
 * lifecycle management by delegating calls to it.
 */
class OTelSpan extends AbstractSpan {

    private final Span otelSpan;

    public OTelSpan(String spanName, Span span, org.opensearch.telemetry.tracing.Span parentSpan) {
        super(spanName, parentSpan);
        this.otelSpan = span;
    }

    @Override
    public void endSpan() {
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
