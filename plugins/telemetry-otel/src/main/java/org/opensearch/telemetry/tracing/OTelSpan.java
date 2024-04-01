/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.attributes.SamplingAttributes;

import java.util.Optional;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.ReadableSpan;

/**
 * Default implementation of {@link Span} using Otel span. It keeps a reference of OpenTelemetry Span and handles span
 * lifecycle management by delegating calls to it.
 */
class OTelSpan extends AbstractSpan {

    private final Span delegateSpan;

    /**
     * Constructor
     * @param spanName span name
     * @param span the delegate span
     * @param parentSpan the parent span
     */
    public OTelSpan(String spanName, Span span, org.opensearch.telemetry.tracing.Span parentSpan) {
        super(spanName, parentSpan);
        this.delegateSpan = span;
    }

    @Override
    public void endSpan() {
        if (isSpanOutlier()) {
            markParentForSampling();
        }
        delegateSpan.end();
    }

    /*
     * This is added temporarily will remove this after the evaluation framework PR.
     * This Framework will be used to evaluate a span if that is an outlier or not.
     */
    private boolean isSpanOutlier() {
        Optional<Boolean> isSpanSampled = Optional.ofNullable(getAttributeBoolean(SamplingAttributes.SAMPLED.getValue()));
        Optional<String> isSpanInferredSampled = Optional.ofNullable(getAttributeString(SamplingAttributes.SAMPLER.getValue()));

        return isSpanSampled.isPresent()
            && isSpanInferredSampled.isPresent()
            && isSpanInferredSampled.get().equals(SamplingAttributes.INFERRED_SAMPLER.getValue());
    }

    private void markParentForSampling() {
        org.opensearch.telemetry.tracing.Span currentParent = getParentSpan();
        while (currentParent != null && currentParent.getAttributeBoolean(SamplingAttributes.SAMPLED.getValue()) == null) {
            currentParent.addAttribute(SamplingAttributes.SAMPLED.getValue(), true);
            currentParent = currentParent.getParentSpan();
        }
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
    public String getAttributeString(String key) {
        if (delegateSpan != null && delegateSpan instanceof ReadableSpan) return ((ReadableSpan) delegateSpan).getAttribute(
            AttributeKey.stringKey(key)
        );

        return null;
    }

    @Override
    public Boolean getAttributeBoolean(String key) {
        if (delegateSpan != null && delegateSpan instanceof ReadableSpan) {
            return ((ReadableSpan) delegateSpan).getAttribute(AttributeKey.booleanKey(key));
        }

        return null;
    }

    @Override
    public Long getAttributeLong(String key) {
        if (delegateSpan != null && delegateSpan instanceof ReadableSpan) return ((ReadableSpan) delegateSpan).getAttribute(
            AttributeKey.longKey(key)
        );

        return null;
    }

    @Override
    public Double getAttributeDouble(String key) {
        if (delegateSpan != null && delegateSpan instanceof ReadableSpan) return ((ReadableSpan) delegateSpan).getAttribute(
            AttributeKey.doubleKey(key)
        );

        return null;
    }

    io.opentelemetry.api.trace.Span getDelegateSpan() {
        return delegateSpan;
    }
}
