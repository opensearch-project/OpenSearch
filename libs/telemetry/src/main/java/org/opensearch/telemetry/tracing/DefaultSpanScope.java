/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.util.Objects;

/**
 * Default implementation for {@link SpanScope}
 */
public class DefaultSpanScope implements SpanScope {
    private final Span span;
    private final SpanScope previousSpanScope;
    private static final ThreadLocal<SpanScope> spanScopeThreadLocal = new ThreadLocal<>();

    /**
     * Constructor
     * @param span span
     * @param previousSpanScope before attached span scope.
     */
    private DefaultSpanScope(Span span, SpanScope previousSpanScope) {
        this.span = Objects.requireNonNull(span);
        this.previousSpanScope = previousSpanScope;
    }

    /**
     * Creates the SpanScope object.
     * @param span span.
     * @return SpanScope spanScope
     */
    public static SpanScope create(Span span) {
        final SpanScope beforeSpanScope = spanScopeThreadLocal.get();
        SpanScope newSpanScope = new DefaultSpanScope(span, beforeSpanScope);
        spanScopeThreadLocal.set(newSpanScope);
        return newSpanScope;
    }

    @Override
    public void close() {
        spanScopeThreadLocal.set(previousSpanScope);
    }

    @Override
    public Span getSpan() {
        return span;
    }

    static SpanScope getCurrentSpanScope() {
        return spanScopeThreadLocal.get();
    }

}
