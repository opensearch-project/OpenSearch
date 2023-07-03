/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import java.util.Locale;
import org.opensearch.action.ActionListener;
import org.opensearch.telemetry.tracing.Scope;
import org.opensearch.telemetry.tracing.TracerFactory;

/**
 * Handles the tracing scope and delegate the request to the action listener.
 * @param <Response> response.
 */
public class TracingActionListener<Response> implements ActionListener<Response> {

    private final ActionListener<Response> delegate;
    private final Scope scope;
    private final TracerFactory tracerFactory;

    /**
     * Creates instance.
     * @param tracerFactory  tracer factory
     * @param delegate  action listener to be delegated
     * @param scope tracer scope.
     */
    public TracingActionListener(TracerFactory tracerFactory, ActionListener<Response> delegate, Scope scope) {
        this.tracerFactory = tracerFactory;
        this.delegate = delegate;
        this.scope = scope;
    }

    @Override
    public void onResponse(Response response) {
        try (scope) {
            delegate.onResponse(response);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (scope) {
            String message = String.format(Locale.ROOT, "Span ended with an operation failure with message [%s]", e.getMessage());
            // TODO: It also make sense to move addSpanEvent and addSpanAttributes to the Scope (may need to change the name.)
            tracerFactory.getTracer().addSpanEvent(message);
            delegate.onFailure(e);
        }
    }
}
