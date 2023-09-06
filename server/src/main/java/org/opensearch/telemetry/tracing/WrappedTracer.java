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
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.io.IOException;
import java.util.List;
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
        return startSpan(context.getSpanName(), context.getAttributes());
    }

    @Override
    public Span startSpan(String spanName) {
        return startSpan(spanName, Attributes.EMPTY);
    }

    @Override
    public Span startSpan(String spanName, Attributes attributes) {
        return startSpan(spanName, (SpanContext) null, attributes);
    }

    @Override
    public SpanContext getCurrentSpan() {
        Tracer delegateTracer = getDelegateTracer();
        return delegateTracer.getCurrentSpan();
    }

    @Override
    public ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext) {
        return startScopedSpan(spanCreationContext, null);
    }

    @Override
    public ScopedSpan startScopedSpan(SpanCreationContext spanCreationContext, SpanContext parentSpan) {
        return getDelegateTracer().startScopedSpan(spanCreationContext, parentSpan);
    }

    @Override
    public SpanScope withSpanInScope(Span span) {
        return getDelegateTracer().withSpanInScope(span);
    }

    @Override
    public Span startSpan(String spanName, SpanContext parentSpan, Attributes attributes) {
        Tracer delegateTracer = getDelegateTracer();
        return delegateTracer.startSpan(spanName, parentSpan, attributes);
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
    public Span startSpan(String spanName, Map<String, List<String>> headers, Attributes attributes) {
        return defaultTracer.startSpan(spanName, headers, attributes);
    }
}
