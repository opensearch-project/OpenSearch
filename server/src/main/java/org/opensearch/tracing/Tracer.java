/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import java.io.Closeable;

/**
 * Tracer is the interface used to create a {@link Span} and interact with current active {@link Span}.
 * It automatically handles the context propagation between threads, tasks, nodes etc.
 *
 * All methods on the Tracer object are multi-thread safe.
 */
public interface Tracer extends Closeable {

    /**
     * Starts the {@link Span} with given name and level
     *
     * @param spanName span name
     * @param level span tracing level
     */
    void startSpan(String spanName, Level level);

    /**
     * Ends the current active {@link Span}
     *
     */
    void endSpan();

    /**
     * Returns the current active {@link Span}
     *
     * @return current active span
     */
    Span getCurrentSpan();

    /**
     * Adds string attribute to the current active {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addAttribute(String key, String value);

    /**
     * Adds long attribute to the current active {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addAttribute(String key, long value);

    /**
     * Adds double attribute to the current active {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addAttribute(String key, double value);

    /**
     * Adds boolean attribute to the current active {@link Span}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    void addAttribute(String key, boolean value);

    /**
     * Adds an event to the current active {@link Span}.
     *
     * @param event event name
     */
    void addEvent(String event);
}
