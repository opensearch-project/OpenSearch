/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.handler;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.attributes.SamplingAttributes;
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
    private final Tracer tracer;

    /**
     * Constructor.
     *
     * @param delegate delegate
     * @param span span
     * @param tracer tracer
     */
    private TraceableTransportResponseHandler(TransportResponseHandler<T> delegate, Span span, Tracer tracer) {
        this.delegate = Objects.requireNonNull(delegate);
        this.span = Objects.requireNonNull(span);
        this.tracer = Objects.requireNonNull(tracer);
    }

    /**
     * Factory method.
     * @param delegate delegate
     * @param span span
     * @param tracer tracer
     * @return transportResponseHandler
     */
    public static <S extends TransportResponse> TransportResponseHandler<S> create(
        TransportResponseHandler<S> delegate,
        Span span,
        Tracer tracer
    ) {
        if (tracer.isRecording() == true) {
            return new TraceableTransportResponseHandler<S>(delegate, span, tracer);
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
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            String sampleInformation = response.getResponseHeaders().getOrDefault(SamplingAttributes.SAMPLED.getValue(), "");
            if (sampleInformation.equals("true")) {
                span.addAttribute(SamplingAttributes.SAMPLED.getValue(), true);
            }
            span.endSpan();
        } finally {
            delegate.handleResponse(response);
        }
    }

    @Override
    public void handleException(TransportException exp) {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            span.setError(exp);
            span.endSpan();
        } finally {
            delegate.handleException(exp);
        }
    }

    @Override
    public String executor() {
        return delegate.executor();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public void handleRejection(Exception exp) {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            span.setError(exp);
            span.endSpan();
        } finally {
            delegate.handleRejection(exp);
        }
    }
}
