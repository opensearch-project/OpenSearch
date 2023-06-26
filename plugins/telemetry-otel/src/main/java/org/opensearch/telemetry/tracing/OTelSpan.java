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

    private final Span oTelSpan;

    public OTelSpan(String spanName, Span span, org.opensearch.telemetry.tracing.Span parentSpan) {
        super(spanName, parentSpan);
        this.oTelSpan = span;
    }

    @Override
    public void endSpan() {
        oTelSpan.end();
    }

    @Override
    public void addAttribute(String key, String value) {
        oTelSpan.setAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Long value) {
        oTelSpan.setAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Double value) {
        oTelSpan.setAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Boolean value) {
        oTelSpan.setAttribute(key, value);
    }

    @Override
    public void addEvent(String event) {
        oTelSpan.addEvent(event);
    }

    @Override
    public String getTraceId() {
        return oTelSpan.getSpanContext().getTraceId();
    }

    @Override
    public String getSpanId() {
        return oTelSpan.getSpanContext().getSpanId();
    }

    io.opentelemetry.api.trace.Span getoTelSpan() {
        return oTelSpan;
    }

}
