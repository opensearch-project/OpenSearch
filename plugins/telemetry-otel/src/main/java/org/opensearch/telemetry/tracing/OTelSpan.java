/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import org.opensearch.common.SetOnce;

/**
 * Default implementation of {@link Span} using Otel span. It keeps a reference of OpenTelemetry Span and handles span
 * lifecycle management by delegating calls to it.
 */
class OTelSpan extends AbstractSpan {

    private final Span delegateSpan;
    private final SetOnce<Boolean> hasEnded;

    public OTelSpan(String spanName, Span span, org.opensearch.telemetry.tracing.Span parentSpan) {
        super(spanName, parentSpan);
        this.delegateSpan = span;
        this.hasEnded = new SetOnce<>();
    }

    @Override
    public void endSpan() {
        delegateSpan.end();
        hasEnded.set(true);
    }

    @Override
    public void addAttribute(String key, String value) {
        delegateSpan.setAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Long value) {
        delegateSpan.setAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Double value) {
        delegateSpan.setAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Boolean value) {
        delegateSpan.setAttribute(key, value);
    }

    @Override
    public void setError(Exception exception) {
        delegateSpan.setStatus(StatusCode.ERROR, exception.getMessage());
    }

    @Override
    public void addEvent(String event) {
        delegateSpan.addEvent(event);
    }

    @Override
    public String getTraceId() {
        return delegateSpan.getSpanContext().getTraceId();
    }

    @Override
    public String getSpanId() {
        return delegateSpan.getSpanContext().getSpanId();
    }

    // TODO revisit this part
    @Override
    public boolean hasEnded() {
        return Boolean.TRUE.equals(hasEnded.get());
    }

    io.opentelemetry.api.trace.Span getDelegateSpan() {
        return delegateSpan;
    }

}
