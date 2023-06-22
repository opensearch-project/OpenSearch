/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.Optional;

/**
 * Core's ThreadContext based TracerContextStorage implementation
 */
public class ThreadContextBasedTracerContextStorage implements TracerContextStorage<String, Span> {

    private final ThreadContext threadContext;

    private final TracingTelemetry tracingTelemetry;

    public ThreadContextBasedTracerContextStorage(ThreadContext threadContext, TracingTelemetry tracingTelemetry) {
        this.threadContext = threadContext;
        this.tracingTelemetry = tracingTelemetry;
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
