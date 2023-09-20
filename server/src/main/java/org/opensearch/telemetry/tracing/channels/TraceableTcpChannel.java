/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.channels;

import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.listener.TraceableActionListener;
import org.opensearch.transport.TcpChannel;

import java.net.InetSocketAddress;

/**
 * Tracer wrapped {@link TcpChannel}
 */
public class TraceableTcpChannel implements TcpChannel {

    private final TcpChannel delegate;
    private final Span span;
    private final Tracer tracer;

    private final static ActionListener<Void> DUMMY_ACTION_LISTENER = ActionListener.wrap(() -> {});

    /**
     * Constructor.
     * @param delegate delegate
     * @param span span
     * @param tracer tracer
     */
    public TraceableTcpChannel(TcpChannel delegate, Span span, Tracer tracer) {
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
     * @return tcp channel
     */
    public static TcpChannel create(TcpChannel delegate, final Span span, final Tracer tracer) {
        if (FeatureFlags.isEnabled(FeatureFlags.TELEMETRY) == true) {
            TraceableTcpChannel traceableTcpChannel = new TraceableTcpChannel(delegate, span, tracer);
            traceableTcpChannel.addCloseListener(TraceableActionListener.create(DUMMY_ACTION_LISTENER, span, tracer));
            return traceableTcpChannel;
        } else {
            return delegate;
        }
    }

    @Override
    public void close() {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            delegate.close();
        } finally {
            span.endSpan();
        }
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        delegate.addCloseListener(listener);
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public boolean isServerChannel() {
        return delegate.isServerChannel();
    }

    @Override
    public String getProfile() {
        return delegate.getProfile();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return delegate.getLocalAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return delegate.getRemoteAddress();
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            delegate.sendMessage(reference, listener);
        } finally {
            span.endSpan();
        }
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        delegate.addConnectListener(listener);
    }

    @Override
    public ChannelStats getChannelStats() {
        return delegate.getChannelStats();
    }
}
