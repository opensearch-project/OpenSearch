/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 * The default tracer implementation. This class implements the basic logic for span lifecycle and its state management.
 * It also handles tracing context propagation between spans.
 *
 *
 */
public class DefaultTracer implements Tracer {
    static final String THREAD_NAME = "th_name";

    private final TracingTelemetry tracingTelemetry;
    private final TracerContextStorage<String, Span> tracerContextStorage;

    /**
     * Creates DefaultTracer instance
     *
     * @param tracingTelemetry tracing telemetry instance
     * @param tracerContextStorage storage used for storing current span context
     */
    public DefaultTracer(TracingTelemetry tracingTelemetry, TracerContextStorage<String, Span> tracerContextStorage) {
        this.tracingTelemetry = tracingTelemetry;
        this.tracerContextStorage = tracerContextStorage;
    }

    @Override
    public Scope startSpan(String spanName) {
        Span span = createSpan(spanName, getCurrentSpan());
        setCurrentSpanInContext(span);
        addDefaultAttributes(span);
        return new ScopeImpl(() -> endSpan(span));
    }

    @Override
    public void addSpanAttribute(String key, String value) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addAttribute(key, value);
    }

    @Override
    public void addSpanAttribute(String key, long value) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addAttribute(key, value);
    }

    @Override
    public void addSpanAttribute(String key, double value) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addAttribute(key, value);
    }

    @Override
    public void addSpanAttribute(String key, boolean value) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addAttribute(key, value);
    }

    @Override
    public void addSpanEvent(String event) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addEvent(event);
    }

    @Override
    public void close() throws IOException {
        ((Closeable) tracingTelemetry).close();
    }

    // Visible for testing
    Span getCurrentSpan() {
        return tracerContextStorage.get(TracerContextStorage.CURRENT_SPAN);
    }

    private void endSpan(Span span) {
        if (span != null) {
            span.endSpan();
            setCurrentSpanInContext(span.getParentSpan());
        }
    }

    private Span createSpan(String spanName, Span parentSpan) {
        return tracingTelemetry.createSpan(spanName, parentSpan);
    }

    private void setCurrentSpanInContext(Span span) {
        tracerContextStorage.put(TracerContextStorage.CURRENT_SPAN, span);
    }

    /**
     * Adds default attributes in the span
     * @param span the current active span
     */
    protected void addDefaultAttributes(Span span) {
        span.addAttribute(THREAD_NAME, Thread.currentThread().getName());
    }

}
