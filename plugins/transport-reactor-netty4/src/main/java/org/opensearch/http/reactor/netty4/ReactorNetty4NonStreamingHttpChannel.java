/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpResponse;
import org.opensearch.transport.reactor.netty4.Netty4Utils;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import reactor.core.publisher.FluxSink;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

class ReactorNetty4NonStreamingHttpChannel implements HttpChannel {
    private final HttpServerRequest request;
    private final HttpServerResponse response;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private final FluxSink<HttpContent> emitter;

    ReactorNetty4NonStreamingHttpChannel(HttpServerRequest request, HttpServerResponse response, FluxSink<HttpContent> emitter) {
        this.request = request;
        this.response = response;
        this.emitter = emitter;
        this.request.withConnection(connection -> Netty4Utils.addListener(connection.channel().closeFuture(), closeContext));
    }

    @Override
    public boolean isOpen() {
        final AtomicBoolean isOpen = new AtomicBoolean();
        request.withConnection(connection -> isOpen.set(connection.channel().isOpen()));
        return isOpen.get();
    }

    @Override
    public void close() {
        request.withConnection(connection -> connection.channel().close());
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        try {
            emitter.next(createResponse(response));
            listener.onResponse(null);
            emitter.complete();
        } catch (final Exception ex) {
            emitter.error(ex);
            listener.onFailure(ex);
        }
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) response.remoteAddress();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) response.hostAddress();
    }

    FullHttpResponse createResponse(HttpResponse response) {
        return (FullHttpResponse) response;
    }
}
