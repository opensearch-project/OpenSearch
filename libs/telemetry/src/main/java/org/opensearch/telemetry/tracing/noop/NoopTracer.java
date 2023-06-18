/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.noop;

import org.opensearch.telemetry.tracing.Level;
import org.opensearch.telemetry.tracing.Scope;
import org.opensearch.telemetry.tracing.Tracer;

/**
 * No-op implementation of Tracer
 */
public class NoopTracer implements Tracer {

    /**
     * No-op Tracer instance
     */
    public static final Tracer INSTANCE = new NoopTracer();

    private NoopTracer() {}

    @Override
    public Scope startSpan(String spanName, Level level) {
        return Scope.NO_OP;
    }

    @Override
    public void endSpan() {}

    /**
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addSpanAttribute(String key, String value) {

    }

    /**
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addSpanAttribute(String key, long value) {

    }

    /**
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addSpanAttribute(String key, double value) {

    }

    /**
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addSpanAttribute(String key, boolean value) {

    }

    @Override
    public void addSpanEvent(String event) {

    }

    @Override
    public void close() {

    }
}
