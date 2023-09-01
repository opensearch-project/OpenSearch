/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskListener;
import org.opensearch.telemetry.tracing.Span;

import java.util.Objects;

/**
 * Tracer wrapped {@link TaskListener}
 * @param <Response> response.
 */
public class TraceableTaskListener<Response> implements TaskListener<Response> {

    private final TaskListener<Response> delegate;
    private final Span span;

    /**
     * Constructor.
     * @param delegate delegate
     * @param span span
     */
    public TraceableTaskListener(TaskListener<Response> delegate, Span span) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(span);
        this.delegate = delegate;
        this.span = span;
    }

    @Override
    public void onResponse(Task task, Response response) {
        span.endSpan();
        delegate.onResponse(task, response);
    }

    @Override
    public void onFailure(Task task, Exception e) {
        span.setError(e);
        span.endSpan();
        delegate.onFailure(task, e);

    }
}
