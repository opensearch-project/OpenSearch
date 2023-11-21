/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Wrapper implementation of Tracer. This delegates call to right tracer based on the tracer settings
 *
 * @opensearch.internal
 */
@InternalApi
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
    public Span startSpan(SpanCreationContext context) {
        return getDelegateTracer().startSpan(context);
    }

    @Override
    public SpanContext getCurrentSpan() {
        Tracer delegateTracer = getDelegateTracer();
        return delegateTracer.getCurrentSpan();
    }

    @Override
    public ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext) {
        return getDelegateTracer().startScopedSpan(spanCreationContext);
    }

    @Override
    public SpanScope withSpanInScope(Span span) {
        return getDelegateTracer().withSpanInScope(span);
    }

    @Override
    public boolean isRecording() {
        return getDelegateTracer().isRecording();
    }

    @Override
    public void close() throws IOException {
        defaultTracer.close();
    }

    // visible for testing
    Tracer getDelegateTracer() {
        return telemetrySettings.isTracingEnabled() ? defaultTracer : NoopTracer.INSTANCE;
    }

    @Override
    public Span startSpan(SpanCreationContext spanCreationContext, Map<String, Collection<String>> headers) {
        return defaultTracer.startSpan(spanCreationContext, headers);
    }
}
