/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.telemetry.tracing.noop.NoopScopedSpan;

/**
 * An auto-closeable that represents scoped span.
 * It provides interface for all the span operations.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ScopedSpan extends AutoCloseable {
    /**
     * No-op Scope implementation
     */
    ScopedSpan NO_OP = new NoopScopedSpan();

    /**
     * Adds string attribute to the {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addAttribute(String key, String value);

    /**
     * Adds long attribute to the {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addAttribute(String key, long value);

    /**
     * Adds double attribute to the {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addAttribute(String key, double value);

    /**
     * Adds boolean attribute to the {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addAttribute(String key, boolean value);

    /**
     * Adds an event to the {@link Span}.
     *
     * @param event event name
     */
    void addEvent(String event);

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
