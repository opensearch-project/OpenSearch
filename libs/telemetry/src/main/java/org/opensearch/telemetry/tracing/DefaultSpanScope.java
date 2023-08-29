/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Default implementation for {@link SpanScope}
 */
public class DefaultSpanScope implements SpanScope {
    private final Span span;
    private final SpanScope beforeAttachedSpanScope;
    private final Consumer<SpanScope> onCloseConsumer;

    /**
     * Constructor
     * @param span span
     * @param beforeAttachedSpanScope before attached span scope.
     * @param onCloseConsumer close consumer
     */
    public DefaultSpanScope(Span span, SpanScope beforeAttachedSpanScope, Consumer<SpanScope> onCloseConsumer) {
        this.span = Objects.requireNonNull(span);
        this.beforeAttachedSpanScope = beforeAttachedSpanScope;
        this.onCloseConsumer = Objects.requireNonNull(onCloseConsumer);
    }

    @Override
    public void close() {
        onCloseConsumer.accept(beforeAttachedSpanScope);
    }

    @Override
    public Span getSpan() {
        return span;
    }

}
