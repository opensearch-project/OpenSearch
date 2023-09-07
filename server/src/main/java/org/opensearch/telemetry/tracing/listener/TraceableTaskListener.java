/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import org.opensearch.common.util.FeatureFlags;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskListener;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

import java.util.Objects;

/**
 * Tracer wrapped {@link TaskListener}
 * @param <Response> response.
 */
public class TraceableTaskListener<Response> implements TaskListener<Response> {

    private final TaskListener<Response> delegate;
    private final Span span;
    private final Tracer tracer;

    /**
     * Constructor.
     *
     * @param delegate delegate
     * @param span     span
     * @param tracer tracer
     */
    private TraceableTaskListener(TaskListener<Response> delegate, Span span, Tracer tracer) {
        this.delegate = Objects.requireNonNull(delegate);
        this.span = Objects.requireNonNull(span);
        this.tracer = Objects.requireNonNull(tracer);
    }

    /**
     * Factory method.
     * @param delegate delegate
     * @param span span
     * @param tracer tracer
     * @return task listener
     */
    public static <Response> TaskListener<Response> create(TaskListener<Response> delegate, Span span, Tracer tracer) {
        if (FeatureFlags.isEnabled(FeatureFlags.TELEMETRY) == true) {
            return new TraceableTaskListener<Response>(delegate, span, tracer);
        } else {
            return delegate;
        }
    }

    @Override
    public void onResponse(Task task, Response response) {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            delegate.onResponse(task, response);
        } finally {
            span.endSpan();
        }

    }

    @Override
    public void onFailure(Task task, Exception e) {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            delegate.onFailure(task, e);
        } finally {
            span.setError(e);
            span.endSpan();
        }
    }
}
