/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.util.HashMap;
import java.util.Map;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;

/**
 * Default implementation of {@link Span} using Otel span. It keeps a reference of OpenTelemetry Span and handles span
 * lifecycle management by delegating calls to it.
 */
class OTelSpan extends AbstractSpan {

    private final Span delegateSpan;

    private final Map<String, Object> metadata;

    /**
     * Constructor
     * @param spanName span name
     * @param span the delegate span
     * @param parentSpan the parent span
     */
    public OTelSpan(String spanName, Span span, org.opensearch.telemetry.tracing.Span parentSpan) {
        super(spanName, parentSpan);
        this.delegateSpan = span;
        this.metadata = new HashMap<>();
    }

    @Override
    public void endSpan() {
        if (getAttributes().containsKey(TracerContextStorage.SAMPLED)) {
            markParentForSampling();
        }
        delegateSpan.end();
    }

    private void markParentForSampling() {
        org.opensearch.telemetry.tracing.Span current_parent = getParentSpan();
        while (current_parent != null && !current_parent.getAttributes().containsKey(TracerContextStorage.SAMPLED)) {
            current_parent.addAttribute(TracerContextStorage.SAMPLED, true);
            current_parent = current_parent.getParentSpan();
        }
    }

    @Override
    public void addAttribute(String key, String value) {
        delegateSpan.setAttribute(key, value);
        metadata.put(key, value);
    }

    @Override
    public void addAttribute(String key, Long value) {
        delegateSpan.setAttribute(key, value);
        metadata.put(key, value);
    }

    @Override
    public void addAttribute(String key, Double value) {
        delegateSpan.setAttribute(key, value);
        metadata.put(key, value);
    }

    @Override
    public void addAttribute(String key, Boolean value) {
        delegateSpan.setAttribute(key, value);
        metadata.put(key, value);
    }

    @Override
    public void setError(Exception exception) {
        if (exception != null) {
            delegateSpan.setStatus(StatusCode.ERROR, exception.getMessage());
        }
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

    @Override
    public Object getAttribute(String key) {
        return metadata.get(key);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return metadata;
    }

    io.opentelemetry.api.trace.Span getDelegateSpan() {
        return delegateSpan;
    }
}
