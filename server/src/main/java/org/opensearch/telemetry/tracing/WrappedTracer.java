/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.io.IOException;

/**
 * Wrapper implementation of Tracer. This delegates call to right tracer based on the tracer settings
 *
 * @opensearch.internal
 */
final class WrappedTracer implements Tracer {

    private final Tracer defaultTracer;
    private final TelemetrySettings telemetrySettings;

    /**
     * Creates WrappedTracer instance
     *
     * @param telemetrySettings telemetry settings
     * @param defaultTracer default tracer instance
     */
    public WrappedTracer(TelemetrySettings telemetrySettings, Tracer defaultTracer) {
        this.defaultTracer = defaultTracer;
        this.telemetrySettings = telemetrySettings;
    }

    @Override
    public SpanScope startSpan(String spanName) {
        Tracer delegateTracer = getDelegateTracer();
        return delegateTracer.startSpan(spanName);
    }

    @Override
    public void close() throws IOException {
        defaultTracer.close();
    }

    // visible for testing
    Tracer getDelegateTracer() {
        return telemetrySettings.isTracingEnabled() ? defaultTracer : NoopTracer.INSTANCE;
    }
}
