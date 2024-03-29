/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.tracing.attributes.SamplingAttributes;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
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
    /**
     * Current thread name.
     */
    static final String THREAD_NAME = "thread.name";

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
        Span parentSpan = null;
        if (context.getParent() != null) {
            parentSpan = context.getParent().getSpan();
        } else {
            parentSpan = getCurrentSpanInternal();
        }

        Span span = createSpan(context, parentSpan);
        addDefaultAttributes(parentSpan, span);
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
        Span span = startSpan(spanCreationContext);
        SpanScope spanScope = withSpanInScope(span);
        return new DefaultScopedSpan(span, spanScope);
    }

    @Override
    public SpanScope withSpanInScope(Span span) {
        return DefaultSpanScope.create(span, tracerContextStorage).attach();
    }

    @Override
    public boolean isRecording() {
        return true;
    }

    private Span createSpan(SpanCreationContext spanCreationContext, Span parentSpan) {
        return tracingTelemetry.createSpan(spanCreationContext, parentSpan);
    }

    /**
     * Adds default attributes in the span
     * @param span the current active span
     */
    protected void addDefaultAttributes(Span parentSpan, Span span) {
        copyInheritableParentAttributes(parentSpan, span);
        span.addAttribute(THREAD_NAME, Thread.currentThread().getName());
    }

    @Override
    public Span startSpan(SpanCreationContext spanCreationContext, Map<String, Collection<String>> headers) {
        Optional<Span> propagatedSpan = tracingTelemetry.getContextPropagator().extractFromHeaders(headers);
        addRequestAttributeToContext(spanCreationContext, headers);
        return startSpan(spanCreationContext.parent(propagatedSpan.map(SpanContext::new).orElse(null)));
    }

    private void addRequestAttributeToContext(SpanCreationContext spanCreationContext, Map<String, Collection<String>> headers) {
        if (headers != null && headers.containsKey(SamplingAttributes.SAMPLER.getValue())) {
            spanCreationContext.getAttributes()
                .addAttribute(SamplingAttributes.SAMPLER.getValue(), SamplingAttributes.INFERRED_SAMPLER.getValue());
        }
    }

    private void copyInheritableParentAttributes(Span parentSpan, Span currentSpan) {
        // This work as common attribute propagator from parent to child
        if (parentSpan != null) {
            Optional<String> inferredAttribute = Optional.ofNullable(parentSpan.getAttributeString(SamplingAttributes.SAMPLER.getValue()));
            if (inferredAttribute.isPresent()) {
                currentSpan.addAttribute(SamplingAttributes.SAMPLER.getValue(), SamplingAttributes.INFERRED_SAMPLER.getValue());
            }
        }
    }
}
