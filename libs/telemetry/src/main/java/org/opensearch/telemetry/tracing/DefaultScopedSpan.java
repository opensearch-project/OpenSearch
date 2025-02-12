/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.InternalApi;

import java.util.Objects;

/**
 * Default implementation of Scope
 *
 * @opensearch.internal
 */
@InternalApi
final class DefaultScopedSpan implements ScopedSpan {

    private final Span span;

    private final SpanScope spanScope;

    /**
     * Creates Scope instance for the given span
     *
     * @param span underlying span
     * @param spanScope span scope.
     */
    public DefaultScopedSpan(Span span, SpanScope spanScope) {
        this.span = Objects.requireNonNull(span);
        this.spanScope = Objects.requireNonNull(spanScope);
    }

    @Override
    public void addAttribute(String key, String value) {
        span.addAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, long value) {
        span.addAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, double value) {
        span.addAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, boolean value) {
        span.addAttribute(key, value);
    }

    @Override
    public void addEvent(String event) {
        span.addEvent(event);
    }

    @Override
    public void setError(Exception exception) {
        span.setError(exception);
    }

    /**
     * Executes the runnable to end the scope
     */
    @Override
    public void close() {
        span.endSpan();
        spanScope.close();
    }

    /**
     * Returns span.
     * @return the span associated with this scope
     */
    Span getSpan() {
        return span;
    }

    /**
     * Returns {@link SpanScope}
     * @return spanScope
     */
    SpanScope getSpanScope() {
        return spanScope;
    }
}
