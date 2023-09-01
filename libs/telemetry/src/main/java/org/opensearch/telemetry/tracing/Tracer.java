/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.telemetry.tracing.http.HttpTracer;

import java.io.Closeable;

/**
 * Tracer is the interface used to create a {@link Span}
 * It automatically handles the context propagation between threads, tasks, nodes etc.
 *
 * All methods on the Tracer object are multi-thread safe.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Tracer extends HttpTracer, Closeable {
    /**
     * Starts the {@link Span} with given {@link SpanCreationContext}
     *
     * @param context span context
     * @return span, must be closed.
     */
    Span startSpan(SpanCreationContext context);

    /**
     * Starts the {@link Span} with given name
     *
     * @param spanName span name
     * @return span, must be closed.
     */
    Span startSpan(String spanName);

    /**
     * Starts the {@link Span} with given name and attributes. This is required in cases when some attribute based
     * decision needs to be made before starting the span. Very useful in the case of Sampling.
     *
     * @param spanName   span name.
     * @param attributes attributes to be added.
     * @return span, must be closed.
     */
    Span startSpan(String spanName, Attributes attributes);

    /**
     * Starts the {@link Span} with the given name, parent and attributes.
     *
     * @param spanName   span name.
     * @param parentSpan parent span.
     * @param attributes attributes to be added.
     * @return span, must be closed.
     */
    Span startSpan(String spanName, SpanContext parentSpan, Attributes attributes);

    /**
     * Returns the current span.
     * @return current wrapped span.
     */
    SpanContext getCurrentSpan();

    /**
     * Start the span and scoped it. This must be used for scenarios where {@link SpanScope} and {@link Span} lifecycles
     * are same and ends within the same thread where created.
     * @param spanCreationContext span creation context
     * @return scope of the span, must be closed with explicit close or with try-with-resource
     */
    ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext);

    /**
     * Start the span and scoped it. This must be used for scenarios where {@link SpanScope} and {@link Span} lifecycles
     * are same and ends within the same thread where created.
     * @param spanCreationContext span creation context
     * @param parentSpan parent span.
     * @return scope of the span, must be closed with explicit close or with try-with-resource
     */
    ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext, SpanContext parentSpan);

    /**
     * Creates the Span Scope for a current thread. It's mandatory to scope the span just after creation so that it will
     * automatically manage the attach /detach to the current thread.
     * @param span span to be scoped
     * @return ScopedSpan
     */
    SpanScope withSpanInScope(Span span);

}
