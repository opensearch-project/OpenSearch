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
import org.opensearch.telemetry.tracing.SpanScope;

import java.util.Objects;

/**
 * Tracer wrapped {@link TaskListener}
 * @param <Response> response.
 */
public class TraceableTaskListener<Response> implements TaskListener<Response> {

    private final TaskListener<Response> delegate;
    private final SpanScope spanScope;

    /**
     * Constructor.
     * @param delegate delegate
     * @param spanScope span
     */
    public TraceableTaskListener(TaskListener<Response> delegate, SpanScope spanScope) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(spanScope);
        this.delegate = delegate;
        this.spanScope = spanScope;
    }

    @Override
    public void onResponse(Task task, Response response) {
        try (spanScope) {
            delegate.onResponse(task, response);
        }
    }

    @Override
    public void onFailure(Task task, Exception e) {
        try (spanScope) {
            spanScope.setError(e);
            delegate.onFailure(task, e);
        }
    }
}
