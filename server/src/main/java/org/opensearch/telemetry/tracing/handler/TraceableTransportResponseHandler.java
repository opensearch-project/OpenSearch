/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.handler;

import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.Objects;

/**
 * Tracer wrapped {@link TransportResponseHandler}
 * @param <T> TransportResponse
 */
public class TraceableTransportResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

    private final Span span;
    private final TransportResponseHandler<T> delegate;

    /**
     * Constructor.
     * @param span
     * @param delegate
     */
    private TraceableTransportResponseHandler(TransportResponseHandler<T> delegate, Span span) {
        this.delegate = Objects.requireNonNull(delegate);
        this.span = Objects.requireNonNull(span);
    }

    /**
     * Factory method.
     * @param delegate delegate
     * @param span span
     * @return transportResponseHandler
     */
    public static TransportResponseHandler create(TransportResponseHandler delegate, Span span) {
        if (FeatureFlags.isEnabled(FeatureFlags.TELEMETRY) == true) {
            return new TraceableTransportResponseHandler(delegate, span);
        } else {
            return delegate;
        }
    }

    @Override
    public T read(StreamInput in) throws IOException {
        return delegate.read(in);
    }

    @Override
    public void handleResponse(T response) {
        span.endSpan();
        delegate.handleResponse(response);
    }

    @Override
    public void handleException(TransportException exp) {
        span.setError(exp);
        span.endSpan();
        delegate.handleException(exp);
    }

    @Override
    public String executor() {
        return delegate.executor();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
