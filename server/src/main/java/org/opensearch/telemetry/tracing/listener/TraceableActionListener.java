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

import java.util.Objects;

/**
 * Tracer wrapped {@link ActionListener}
 * @param <Response> response.
 */
public class TraceableActionListener<Response> implements ActionListener<Response> {

    private final ActionListener<Response> delegate;
    private final Span span;

    /**
     * Constructor.
     * @param delegate delegate
     * @param span span
     */
    public TraceableActionListener(ActionListener<Response> delegate, Span span) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(span);
        this.delegate = delegate;
        this.span = span;
    }

    @Override
    public void onResponse(Response response) {
        span.endSpan();
        delegate.onResponse(response);
    }

    @Override
    public void onFailure(Exception e) {
        span.setError(e);
        span.endSpan();
        delegate.onFailure(e);
    }
}
