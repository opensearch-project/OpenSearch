/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContextStatePropagator;
import org.opensearch.telemetry.tracing.attributes.SamplingAttributes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Core's ThreadContext based TracerContextStorage implementation
 *
 * @opensearch.internal
 */
@InternalApi
public class ThreadContextBasedTracerContextStorage implements TracerContextStorage<String, Span>, ThreadContextStatePropagator {

    private final ThreadContext threadContext;

    private final TracingTelemetry tracingTelemetry;

    public ThreadContextBasedTracerContextStorage(ThreadContext threadContext, TracingTelemetry tracingTelemetry) {
        this.threadContext = Objects.requireNonNull(threadContext);
        this.tracingTelemetry = Objects.requireNonNull(tracingTelemetry);
        this.threadContext.registerThreadContextStatePropagator(this);
    }

    @Override
    public Span get(String key) {
        return getCurrentSpan(key);
    }

    @Override
    public void put(String key, Span span) {
        SpanReference currentSpanRef = threadContext.getTransient(key);
        if (currentSpanRef == null) {
            threadContext.putTransient(key, new SpanReference(span));
        } else {
            currentSpanRef.setSpan(span);
        }
    }

    @Override
    @SuppressWarnings("removal")
    public Map<String, Object> transients(Map<String, Object> source) {
        final Map<String, Object> transients = new HashMap<>();
        if (source.containsKey(CURRENT_SPAN)) {
            final SpanReference current = (SpanReference) source.get(CURRENT_SPAN);
            if (current != null) {
                transients.put(CURRENT_SPAN, new SpanReference(current.getSpan()));
            }
        }
        return transients;
    }

    @Override
    public Map<String, Object> transients(Map<String, Object> source, boolean isSystemContext) {
        if (isSystemContext == true) {
            return Collections.emptyMap();
        } else {
            return transients(source);
        }
    }

    @Override
    @SuppressWarnings("removal")
    public Map<String, String> headers(Map<String, Object> source) {
        final Map<String, String> headers = new HashMap<>();

        if (source.containsKey(CURRENT_SPAN)) {
            final SpanReference current = (SpanReference) source.get(CURRENT_SPAN);
            if (current != null && current.getSpan() != null) {
                tracingTelemetry.getContextPropagator().inject(current.getSpan(), headers::put);

                // We will be sending one more header with the response if the request is marked for sampling
                Optional<Boolean> isSpanSampled = Optional.ofNullable(
                    current.getSpan().getAttributeBoolean(SamplingAttributes.SAMPLED.getValue())
                );
                if (isSpanSampled.isPresent()) {
                    headers.put(SamplingAttributes.SAMPLED.getValue(), "true");
                }
                Optional<String> isSpanInferredSampled = Optional.ofNullable(
                    current.getSpan().getAttributeString(SamplingAttributes.SAMPLER.getValue())
                );
                if (isSpanInferredSampled.isPresent()
                    && isSpanInferredSampled.get().equals(SamplingAttributes.INFERRED_SAMPLER.getValue())) {
                    headers.put(SamplingAttributes.SAMPLER.getValue(), isSpanInferredSampled.get());
                }
            }
        }

        return headers;
    }

    @Override
    public Map<String, String> headers(Map<String, Object> source, boolean isSystemContext) {
        return headers(source);
    }

    Span getCurrentSpan(String key) {
        SpanReference currentSpanRef = threadContext.getTransient(key);
        return (currentSpanRef == null) ? null : currentSpanRef.getSpan();
    }
}
