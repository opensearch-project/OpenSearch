/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.util.function.Consumer;

/**
 * Default implementation of Scope
 *
 * @opensearch.internal
 */
final class DefaultSpanScope implements SpanScope {

    private final Span span;

    private final Consumer<Span> onCloseConsumer;

    /**
     * Creates Scope instance for the given span
     *
     * @param span underlying span
     * @param onCloseConsumer consumer to execute on scope close
     */
    public DefaultSpanScope(Span span, Consumer<Span> onCloseConsumer) {
        this.span = span;
        this.onCloseConsumer = onCloseConsumer;
    }

    @Override
    public void addSpanAttribute(String key, String value) {
        span.addAttribute(key, value);
    }

    @Override
    public void addSpanAttribute(String key, long value) {
        span.addAttribute(key, value);
    }

    @Override
    public void addSpanAttribute(String key, double value) {
        span.addAttribute(key, value);
    }

    @Override
    public void addSpanAttribute(String key, boolean value) {
        span.addAttribute(key, value);
    }

    @Override
    public void addSpanEvent(String event) {
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
        onCloseConsumer.accept(span);
    }
}
