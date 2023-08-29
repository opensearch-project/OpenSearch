/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.attributes.Attributes;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 * The default tracer implementation. It handles tracing context propagation between spans by maintaining
 * current active span in its storage
 *
 *  @opensearch.internal
 */
class DefaultTracer implements Tracer {
    static final String THREAD_NAME = "th_name";

    private final TracingTelemetry tracingTelemetry;
    private final TracerContextStorage<String, Span> spanTracerContextStorage;
    private final TracerContextStorage<String, SpanScope> spanScopeTracerContextStorage;

    /**
     * Creates DefaultTracer instance
     *
     * @param tracingTelemetry tracing telemetry instance
     * @param spanTracerContextStorage storage used for storing current span context
     */
    public DefaultTracer(
        TracingTelemetry tracingTelemetry,
        TracerContextStorage<String, Span> spanTracerContextStorage,
        TracerContextStorage<String, SpanScope> spanScopeTracerContextStorage
    ) {
        this.tracingTelemetry = tracingTelemetry;
        this.spanTracerContextStorage = spanTracerContextStorage;
        this.spanScopeTracerContextStorage = spanScopeTracerContextStorage;
    }

    @Override
    public Span startSpan(SpanCreationContext context) {
        return startSpan(context.getSpanName(), context.getAttributes());
    }

    @Override
    public Span startSpan(String spanName) {
        return startSpan(spanName, Attributes.EMPTY);
    }

    @Override
    public Span startSpan(String spanName, Attributes attributes) {
        return startSpan(spanName, (SpanContext) null, attributes);
    }

    @Override
    public Span startSpan(String spanName, SpanContext parentSpan, Attributes attributes) {
        Span span = null;
        if (parentSpan != null) {
            span = createSpan(spanName, parentSpan.getSpan(), attributes);
        } else {
            span = createSpan(spanName, getCurrentSpanInternal(), attributes);
        }
        setCurrentSpanInContext(span);
        addDefaultAttributes(span);
        return span;
    }

    @Override
    public void close() throws IOException {
        ((Closeable) tracingTelemetry).close();
    }

    private Span getCurrentSpanInternal() {
        return spanTracerContextStorage.get(TracerContextStorage.CURRENT_SPAN);
    }

    public SpanContext getCurrentSpan() {
        final Span currentSpan = spanTracerContextStorage.get(TracerContextStorage.CURRENT_SPAN);
        return (currentSpan == null) ? null : new SpanContext(currentSpan);
    }

    private SpanScope getCurrentSpanScope() {
        return spanScopeTracerContextStorage.get(TracerContextStorage.CURRENT_SPAN_SCOPE);
    }

    @Override
    public ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext) {
        Span span = startSpan(spanCreationContext);
        SpanScope spanScope = createSpanScope(span);
        return new DefaultScopedSpan(
            span,
            spanScope,
            (spanToBeClosed, scopeSpanToBeClosed) -> endScopedSpan(spanToBeClosed, scopeSpanToBeClosed)
        );
    }

    @Override
    public ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext, SpanContext parentSpan) {
        Span span = startSpan(spanCreationContext.getSpanName(), parentSpan, spanCreationContext.getAttributes());
        SpanScope spanScope = createSpanScope(span);
        return new DefaultScopedSpan(
            span,
            spanScope,
            (spanToBeClosed, scopeSpanToBeClosed) -> endScopedSpan(spanToBeClosed, scopeSpanToBeClosed)
        );
    }

    @Override
    public SpanScope createSpanScope(Span span) {
        SpanScope defaultSpanScope = new DefaultSpanScope(span, getCurrentSpanScope(), (spanScope) -> closeSpanScope(spanScope));
        setCurrentSpanScopeInContext(defaultSpanScope);
        return defaultSpanScope;
    }

    private void closeSpanScope(SpanScope beforeAttachedSpanScope) {
        setCurrentSpanScopeInContext(beforeAttachedSpanScope);
        if (beforeAttachedSpanScope != null) {
            setCurrentSpanInContext(beforeAttachedSpanScope.getSpan());
        } else {
            setCurrentSpanInContext(null);
        }
    }

    private void setCurrentSpanScopeInContext(SpanScope spanScope) {
        spanScopeTracerContextStorage.put(TracerContextStorage.CURRENT_SPAN_SCOPE, spanScope);
    }

    private void endScopedSpan(Span span, SpanScope spanScope) {
        endSpan(span);
        spanScope.close();
    }

    private void endSpan(Span span) {
        if (span != null) {
            span.endSpan();
            setCurrentSpanInContext(span.getParentSpan());
        }
    }

    private Span createSpan(String spanName, Span parentSpan, Attributes attributes) {
        return tracingTelemetry.createSpan(spanName, parentSpan, attributes);
    }

    private void setCurrentSpanInContext(Span span) {
        spanTracerContextStorage.put(TracerContextStorage.CURRENT_SPAN, span);
    }

    /**
     * Adds default attributes in the span
     * @param span the current active span
     */
    protected void addDefaultAttributes(Span span) {
        span.addAttribute(THREAD_NAME, Thread.currentThread().getName());
    }

    @Override
    public Span startSpan(String spanName, Map<String, List<String>> headers, Attributes attributes) {
        Optional<Span> propagatedSpan = tracingTelemetry.getContextPropagator().extractFromHeaders(headers);
        return startSpan(spanName, propagatedSpan.map(SpanContext::new).orElse(null), attributes);
    }

}
