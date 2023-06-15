/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.noop.NoopSpan;

import java.util.function.Supplier;

/**
 * Factory to create spans based on the configured level
 */
public final class SpanFactory {

    private final TracingTelemetry tracingTelemetry;

    private final Supplier<Level> levelSupplier;

    /**
     * Creates SpanFactor instance with provided level supplier and tracing telemetry
     * @param levelSupplier configured level supplier
     * @param tracingTelemetry tracing telemetry
     */
    public SpanFactory(Supplier<Level> levelSupplier, TracingTelemetry tracingTelemetry) {
        this.levelSupplier = levelSupplier;
        this.tracingTelemetry = tracingTelemetry;
    }

    /**
     * Creates span with provided arguments
     * @param spanName name of the span
     * @param parentSpan span's parent span
     * @param level of the span
     * @return span instance
     */
    public Span createSpan(String spanName, Span parentSpan, Level level) {
        return isLevelEnabled(level) ? createDefaultSpan(spanName, parentSpan, level) : createNoopSpan(spanName, parentSpan, level);
    }

    private boolean isLevelEnabled(Level level) {
        Level configuredLevel = levelSupplier.get();
        return level.isHigherOrEqual(configuredLevel);
    }

    private Span createDefaultSpan(String spanName, Span parentSpan, Level level) {
        Span telemetrySpan = tracingTelemetry.createSpan(spanName, getLastValidSpanInChain(parentSpan), level);
        return telemetrySpan;
    }

    private NoopSpan createNoopSpan(String spanName, Span parentSpan, Level level) {
        return new NoopSpan(spanName, parentSpan, level);
    }

    private Span getLastValidSpanInChain(Span parentSpan) {
        while (parentSpan instanceof NoopSpan) {
            parentSpan = parentSpan.getParentSpan();
        }
        return parentSpan;
    }
}
