/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import org.opensearch.core.action.ActionListener;
import org.opensearch.telemetry.tracing.SpanScope;

import java.util.Objects;

/**
 * Tracer wrapped {@link ActionListener}
 * @param <Response> response.
 */
public class TraceableActionListener<Response> implements ActionListener<Response> {

    private final ActionListener<Response> delegate;
    private final SpanScope spanScope;

    /**
     * Constructor.
     * @param delegate delegate
     * @param spanScope span
     */
    public TraceableActionListener(ActionListener<Response> delegate, SpanScope spanScope) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(spanScope);
        this.delegate = delegate;
        this.spanScope = spanScope;
    }

    @Override
    public void onResponse(Response response) {
        try (spanScope) {
            // closing the span before
        }
        delegate.onResponse(response);
    }

    @Override
    public void onFailure(Exception e) {
        try (spanScope) {
            spanScope.setError(e);
        }
        delegate.onFailure(e);
    }
}
