/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.trace.SpanContext;

/**
 * Default implementation of {@link Span} using Otel span. It keeps a reference of OpenTelemetry Span and handles span
 * lifecycle management by delegating calls to it.
 */
class OTelSpan implements Span {

    private final String spanName;
    private final io.opentelemetry.api.trace.Span otelSpan;
    private final Span parentSpan;
    private final Level level;

    public OTelSpan(String spanName, io.opentelemetry.api.trace.Span span, Span parentSpan, Level level) {
        this.spanName = spanName;
        this.otelSpan = span;
        this.parentSpan = parentSpan;
        this.level = level;
    }

    @Override
    public Span getParentSpan() {
        return parentSpan;
    }

    @Override
    public Level getLevel() {
        return level;
    }

    @Override
    public String getSpanName() {
        return spanName;
    }

    io.opentelemetry.api.trace.Span getOtelSpan() {
        return otelSpan;
    }

    SpanContext getSpanContext() {
        return otelSpan.getSpanContext();
    }

}
