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
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpResponse;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.listener.TraceableActionListener;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Tracer wrapped {@link HttpChannel}
 */
public class TraceableHttpChannel implements HttpChannel {
    private final HttpChannel delegate;
    private final Span span;

    /**
     * Constructor.
     *
     * @param delegate  delegate
     * @param span span
     */
    private TraceableHttpChannel(HttpChannel delegate, Span span) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(span);
        this.span = span;
        this.delegate = delegate;
    }

    /**
     * Factory method.
     * @param delegate delegate
     * @param span span
     * @return http channel
     */
    public static HttpChannel create(HttpChannel delegate, Span span) {
        if (FeatureFlags.isEnabled(FeatureFlags.TELEMETRY)) {
            return new TraceableHttpChannel(delegate, span);
        } else {
            return delegate;
        }
    }

    @Override
    public void close() {
        delegate.close();
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
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        delegate.sendResponse(response, TraceableActionListener.create(listener, span));
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return delegate.getLocalAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return delegate.getRemoteAddress();
    }
}
