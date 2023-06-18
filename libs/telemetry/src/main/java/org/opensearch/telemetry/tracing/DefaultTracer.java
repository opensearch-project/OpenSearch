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
import java.util.function.Supplier;

/**
 *
 * The default tracer implementation. This class implements the basic logic for span lifecycle and its state management.
 * It also handles tracing context propagation between spans.
 *
 *
 */
public class DefaultTracer implements Tracer {

    /**
     * Key for storing current span
     */
    public static final String CURRENT_SPAN = "current_span";

    static final String THREAD_NAME = "th_name";

    private final TracingTelemetry tracingTelemetry;
    private final TracerContextStorage<String, Span> tracerContextStorage;
    private final Supplier<Level> levelSupplier;
    private final SpanFactory spanFactory;

    /**
     * Creates DefaultTracer instance
     *
     * @param tracingTelemetry tracing telemetry instance
     * @param tracerContextStorage storage used for storing current span context
     * @param levelSupplier configured level supplier
     */
    public DefaultTracer(
        TracingTelemetry tracingTelemetry,
        TracerContextStorage<String, Span> tracerContextStorage,
        Supplier<Level> levelSupplier
    ) {
        this.tracingTelemetry = tracingTelemetry;
        this.tracerContextStorage = tracerContextStorage;
        this.levelSupplier = levelSupplier;
        this.spanFactory = new SpanFactory(levelSupplier, tracingTelemetry);
    }

    @Override
    public Scope startSpan(String spanName, Level level) {
        Span span = createSpan(spanName, getCurrentSpan(), level);
        setCurrentSpanInContext(span);
        addDefaultAttributes(span);
        return new ScopeImpl(() -> endSpan());
    }

    @Override
    public void endSpan() {
        Span currentSpan = getCurrentSpan();
        if (currentSpan != null) {
            currentSpan.endSpan();
            setCurrentSpanInContext(currentSpan.getParentSpan());
        }
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
        return tracerContextStorage.get(CURRENT_SPAN);
    }

    private Span createSpan(String spanName, Span parentSpan, Level level) {
        return spanFactory.createSpan(spanName, parentSpan, level);
    }

    private void setCurrentSpanInContext(Span span) {
        tracerContextStorage.put(CURRENT_SPAN, span);
    }

    private void addDefaultAttributes(Span span) {
        span.addAttribute(THREAD_NAME, Thread.currentThread().getName());
    }

}
