/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.telemetry.tracing.transport.TransportTracer;

import java.io.Closeable;

/**
 * Tracer is the interface used to create a {@link Span}
 * It automatically handles the context propagation between threads, tasks, nodes etc.
 * <p>
 * All methods on the Tracer object are multi-thread safe.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Tracer extends TransportTracer, Closeable {
    /**
     * Starts the {@link Span} with given {@link SpanCreationContext}
     *
     * @param context span context
     * @return span, must be closed.
     */
    Span startSpan(SpanCreationContext context);

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
     * Creates the Span Scope for a current thread. It's mandatory to scope the span just after creation so that it will
     * automatically manage the attach /detach to the current thread.
     * @param span span to be scoped
     * @return ScopedSpan
     */
    SpanScope withSpanInScope(Span span);

    /**
     * Tells if the traces are being recorded or not
     * @return boolean
     */
    boolean isRecording();

}
