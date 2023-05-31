/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import org.opensearch.tracing.noop.NoopSpan;

/**
 * Factory to create spans based on the configured level
 */
public class SpanFactory {

    private final TracerSettings tracerSettings;

    private final Telemetry telemetry;

    public SpanFactory(TracerSettings tracerSettings, Telemetry telemetry) {
        this.tracerSettings = tracerSettings;
        this.telemetry = telemetry;
    }

    public Span createSpan(String spanName, Span parentSpan, Level level) {
        return isLevelEnabled(level) ? createDefaultSpan(spanName, parentSpan, level) : createNoopSpan(spanName, parentSpan, level);
    }

    private boolean isLevelEnabled(Level level) {
        Level configuredLevel = tracerSettings.getTracerLevel();
        return level.isHigherOrEqual(configuredLevel);
    }

    private Span createDefaultSpan(String spanName, Span parentSpan, Level level) {
        Span telemetrySpan = telemetry.createSpan(spanName, getLastValidSpanInChain(parentSpan), level);
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
