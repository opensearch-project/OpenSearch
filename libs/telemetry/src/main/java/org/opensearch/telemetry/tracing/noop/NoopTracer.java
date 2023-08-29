/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.noop;

import org.opensearch.telemetry.tracing.SpanContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.attributes.Attributes;

import java.util.List;
import java.util.Map;

/**
 * No-op implementation of Tracer
 *
 * @opensearch.internal
 */
public class NoopTracer implements Tracer {

    /**
     * No-op Tracer instance
     */
    public static final Tracer INSTANCE = new NoopTracer();

    private NoopTracer() {}

    @Override
    public SpanScope startSpan(String spanName) {
        return SpanScope.NO_OP;
    }

    @Override
    public SpanScope startSpan(String spanName, Attributes attributes) {
        return SpanScope.NO_OP;
    }

    @Override
    public SpanScope startSpan(String spanName, SpanContext parentSpan, Attributes attributes) {
        return SpanScope.NO_OP;
    }

    @Override
    public SpanContext getCurrentSpan() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public SpanScope startSpan(String spanName, Map<String, List<String>> header, Attributes attributes) {
        return SpanScope.NO_OP;
    }
}
