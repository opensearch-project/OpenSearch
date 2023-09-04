/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import org.opensearch.common.util.FeatureFlags;
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
    private TraceableActionListener(ActionListener<Response> delegate, Span span) {
        this.delegate = Objects.requireNonNull(delegate);
        this.span = Objects.requireNonNull(span);
    }

    /**
     * Factory method.
     * @param delegate delegate
     * @param span span
     * @return action listener
     */
    public static ActionListener create(ActionListener delegate, Span span) {
        if (FeatureFlags.isEnabled(FeatureFlags.TELEMETRY) == true) {
            return new TraceableActionListener(delegate, span);
        } else {
            return delegate;
        }
    }

    @Override
    public void onResponse(Response response) {
        try {
            delegate.onResponse(response);
        } finally {
            span.endSpan();
        }

    }

    @Override
    public void onFailure(Exception e) {
        try {
            delegate.onFailure(e);
        } finally {
            span.setError(e);
            span.endSpan();
        }

    }
}
