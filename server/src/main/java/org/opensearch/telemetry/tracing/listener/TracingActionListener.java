/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import org.opensearch.action.ActionListener;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.TracerFactory;

/**
 * Handles the tracing scope and delegate the request to the action listener.
 * @param <Response> response.
 */
public class TracingActionListener<Response> implements ActionListener<Response> {

    private final ActionListener<Response> delegate;
    private final SpanScope spanScope;
    private final TracerFactory tracerFactory;

    /**
     * Creates instance.
     * @param tracerFactory  tracer factory
     * @param delegate  action listener to be delegated
     * @param spanScope tracer scope.
     */
    public TracingActionListener(TracerFactory tracerFactory, ActionListener<Response> delegate, SpanScope spanScope) {
        this.tracerFactory = tracerFactory;
        this.delegate = delegate;
        this.spanScope = spanScope;
    }

    @Override
    public void onResponse(Response response) {
        try (spanScope) {
            delegate.onResponse(response);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (spanScope) {
            spanScope.setError(e);
            delegate.onFailure(e);
        }
    }
}
