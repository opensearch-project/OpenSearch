/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.channels;

import org.opensearch.Version;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportChannel;

import java.io.IOException;

/**
 * Tracer wrapped {@link TransportChannel}
 */
public class TraceableTransportChannel implements TransportChannel {

    private final TransportChannel delegate;
    private final Span span;
    private final Tracer tracer;

    /**
     * Constructor.
     * @param delegate delegate
     * @param span span
     * @param tracer tracer
     */
    public TraceableTransportChannel(TransportChannel delegate, Span span, Tracer tracer, TcpChannel tcpChannel) {
        this.delegate = delegate;
        this.span = span;
        this.tracer = tracer;
    }

    /**
     * Factory method.
     *
     * @param delegate delegate
     * @param span     span
     * @param tracer tracer
     * @return transport channel
     */
    public static TransportChannel create(TransportChannel delegate, final Span span, final Tracer tracer, final TcpChannel tcpChannel) {
        if (FeatureFlags.isEnabled(FeatureFlags.TELEMETRY) == true) {
            tcpChannel.addCloseListener(new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    onFailure(null);
                }

                @Override
                public void onFailure(Exception e) {
                    span.addEvent("The TransportChannel was closed without sending the response");
                    span.setError(e);
                    span.endSpan();
                }
            });

            return new TraceableTransportChannel(delegate, span, tracer, tcpChannel);
        } else {
            return delegate;
        }
    }

    @Override
    public String getProfileName() {
        return delegate.getProfileName();
    }

    @Override
    public String getChannelType() {
        return delegate.getChannelType();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            delegate.sendResponse(response);
        } catch (final IOException ex) {
            span.setError(ex);
            throw ex;
        } finally {
            span.endSpan();
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            delegate.sendResponse(exception);
        } finally {
            span.setError(exception);
            span.endSpan();
        }
    }

    @Override
    public Version getVersion() {
        return delegate.getVersion();
    }
}
