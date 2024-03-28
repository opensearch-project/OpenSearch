/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import org.opensearch.core.action.ActionListener;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

import java.util.Objects;

/**
 * Tracer wrapped {@link ActionListener}
 * @param <Response> response.
 */
public class TraceableActionListener<Response> implements ActionListener<Response> {

    private final ActionListener<Response> delegate;
    private final Span span;
    private final Tracer tracer;

    /**
     * Constructor.
     *
     * @param delegate delegate
     * @param span     span
     * @param tracer tracer
     */
    private TraceableActionListener(ActionListener<Response> delegate, Span span, Tracer tracer) {
        this.delegate = Objects.requireNonNull(delegate);
        this.span = Objects.requireNonNull(span);
        this.tracer = Objects.requireNonNull(tracer);
    }

    /**
     * Factory method.
     * @param delegate delegate
     * @param span span
     * @param tracer tracer
     * @return action listener
     */
    public static <Response> ActionListener<Response> create(ActionListener<Response> delegate, Span span, Tracer tracer) {
        if (tracer.isRecording() == true) {
            return new TraceableActionListener<Response>(delegate, span, tracer);
        } else {
            return delegate;
        }
    }

    @Override
    public void onResponse(Response response) {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            span.endSpan();
        } finally {
            delegate.onResponse(response);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            span.setError(e);
            span.endSpan();
        } finally {
            delegate.onFailure(e);
        }
    }
}
