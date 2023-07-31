/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContextStatePropagator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Core's ThreadContext based TracerContextStorage implementation
 *
 * @opensearch.internal
 */
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
        if (span == null) {
            return;
        }
        SpanReference currentSpanRef = threadContext.getTransient(key);
        if (currentSpanRef == null) {
            threadContext.putTransient(key, new SpanReference(span));
        } else {
            currentSpanRef.setSpan(span);
        }
    }

    @Override
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
    public Map<String, String> headers(Map<String, Object> source) {
        final Map<String, String> headers = new HashMap<>();

        if (source.containsKey(CURRENT_SPAN)) {
            final SpanReference current = (SpanReference) source.get(CURRENT_SPAN);
            if (current != null) {
                tracingTelemetry.getContextPropagator().inject(current.getSpan(), (key, value) -> headers.put(key, value));
            }
        }

        return headers;
    }

    Span getCurrentSpan(String key) {
        Optional<Span> optionalSpanFromContext = spanFromThreadContext(key);
        return optionalSpanFromContext.orElse(spanFromHeader());
    }

    private Optional<Span> spanFromThreadContext(String key) {
        SpanReference currentSpanRef = threadContext.getTransient(key);
        return (currentSpanRef == null) ? Optional.empty() : Optional.ofNullable(currentSpanRef.getSpan());
    }

    private Span spanFromHeader() {
        return tracingTelemetry.getContextPropagator().extract(threadContext.getHeaders());
    }
}
