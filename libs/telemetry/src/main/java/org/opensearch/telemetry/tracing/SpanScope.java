/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.noop.NoopSpanScope;

/**
 * An auto-closeable that represents scope of the span.
 * It provides interface for all the span operations.
 */
public interface SpanScope extends AutoCloseable {
    /**
     * No-op Scope implementation
     */
    SpanScope NO_OP = new NoopSpanScope();

    /**
     * Adds string attribute to the {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addSpanAttribute(String key, String value);

    /**
     * Adds long attribute to the {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addSpanAttribute(String key, long value);

    /**
     * Adds double attribute to the {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addSpanAttribute(String key, double value);

    /**
     * Adds boolean attribute to the {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addSpanAttribute(String key, boolean value);

    /**
     * Adds an event to the {@link Span}.
     *
     * @param event event name
     */
    void addSpanEvent(String event);

    /**
     * Records error in the span
     *
     * @param exception exception to be recorded
     */
    void setError(Exception exception);

    /**
     * closes the scope
     */
    @Override
    void close();
}
