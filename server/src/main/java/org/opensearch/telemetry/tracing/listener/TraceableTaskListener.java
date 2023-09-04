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
    private TraceableTaskListener(TaskListener<Response> delegate, Span span) {
        this.delegate = Objects.requireNonNull(delegate);
        this.span = Objects.requireNonNull(span);
    }

    /**
     * Factory method.
     * @param delegate delegate
     * @param span span
     * @return task listener
     */
    public static TaskListener create(TaskListener delegate, Span span) {
        if (FeatureFlags.isEnabled(FeatureFlags.TELEMETRY) == true) {
            return new TraceableTaskListener(delegate, span);
        } else {
            return delegate;
        }
    }

    @Override
    public void onResponse(Task task, Response response) {
        try {
            delegate.onResponse(task, response);
        } finally {
            span.endSpan();
        }

    }

    @Override
    public void onFailure(Task task, Exception e) {
        try {
            delegate.onFailure(task, e);
        } finally {
            span.setError(e);
            span.endSpan();
        }
    }
}
