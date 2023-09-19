/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.InternalApi;
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
@InternalApi
class DefaultTracer implements Tracer {
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
    public Span startSpan(SpanCreationContext context) {
        return startSpan(context.getSpanName(), context.getAttributes(), context.getSpanKind());
    }

    @Override
    public Span startSpan(String spanName, SpanKind spanKind) {
        return startSpan(spanName, Attributes.EMPTY, spanKind);
    }

    @Override
    public Span startSpan(String spanName, Attributes attributes, SpanKind spanKind) {
        return startSpan(spanName, (SpanContext) null, attributes, spanKind);
    }

    @Override
    public Span startSpan(String spanName, SpanContext parentSpan, Attributes attributes, SpanKind spanKind) {
        Span span = null;
        if (parentSpan != null) {
            span = createSpan(spanName, parentSpan.getSpan(), attributes, spanKind);
        } else {
            span = createSpan(spanName, getCurrentSpanInternal(), attributes, spanKind);
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
        return tracerContextStorage.get(TracerContextStorage.CURRENT_SPAN);
    }

    @Override
    public SpanContext getCurrentSpan() {
        final Span currentSpan = tracerContextStorage.get(TracerContextStorage.CURRENT_SPAN);
        return (currentSpan == null) ? null : new SpanContext(currentSpan);
    }

    @Override
    public ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext) {
        return startScopedSpan(spanCreationContext, null);
    }

    @Override
    public ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext, SpanContext parentSpan) {
        Span span = startSpan(
            spanCreationContext.getSpanName(),
            parentSpan,
            spanCreationContext.getAttributes(),
            spanCreationContext.getSpanKind()
        );
        SpanScope spanScope = withSpanInScope(span);
        return new DefaultScopedSpan(span, spanScope);
    }

    @Override
    public SpanScope withSpanInScope(Span span) {
        return DefaultSpanScope.create(span, tracerContextStorage).attach();
    }

    private Span createSpan(String spanName, Span parentSpan, Attributes attributes, SpanKind spanKind) {
        return tracingTelemetry.createSpan(spanName, parentSpan, attributes, spanKind);
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

    @Override
    public Span startSpan(SpanCreationContext spanCreationContext, Map<String, List<String>> headers) {
        Optional<Span> propagatedSpan = tracingTelemetry.getContextPropagator().extractFromHeaders(headers);
        return startSpan(
            spanCreationContext.getSpanName(),
            propagatedSpan.map(SpanContext::new).orElse(null),
            spanCreationContext.getAttributes(),
            spanCreationContext.getSpanKind()
        );
    }

}
