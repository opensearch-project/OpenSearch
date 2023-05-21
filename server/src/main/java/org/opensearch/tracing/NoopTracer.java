/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

/**
 * No-op implementation of Tracer
 */
class NoopTracer implements Tracer {

    @Override
    public void startSpan(String spanName, Level level) {

    }

    @Override
    public void endSpan() {}

    /**
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addAttribute(String key, String value) {

    }

    /**
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addAttribute(String key, long value) {

    }

    /**
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addAttribute(String key, double value) {

    }

    /**
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addAttribute(String key, boolean value) {

    }

    @Override
    public void addEvent(String event) {

    }

    @Override
    public Span getCurrentSpan() {
        return null;
    }

    @Override
    public void close() {

    }
}
