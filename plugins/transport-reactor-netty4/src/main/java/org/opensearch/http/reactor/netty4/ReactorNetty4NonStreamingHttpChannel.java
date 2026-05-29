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
import org.opensearch.http.reactor.netty4.ReactorNetty4HttpServerTransport.HostChannel;

import java.net.InetSocketAddress;
import java.util.Optional;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import reactor.core.publisher.FluxSink;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

class ReactorNetty4NonStreamingHttpChannel implements HttpChannel {
    private final HttpServerRequest request;
    private final HttpServerResponse response;
    private final FluxSink<HttpContent> emitter;
    private final HostChannel hostChannel;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();

    ReactorNetty4NonStreamingHttpChannel(
        HostChannel hostChannel,
        HttpServerRequest request,
        HttpServerResponse response,
        FluxSink<HttpContent> emitter
    ) {
        this.hostChannel = hostChannel;
        this.request = request;
        this.response = response;
        this.emitter = emitter;
    }

    @Override
    public boolean isOpen() {
        return hostChannel.isOpen();
    }

    @Override
    public void close() {
        if (closeContext.isDone() == false) {
            closeContext.complete(null);
            hostChannel.close(this);
        }
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

    @Override
    public <T> Optional<T> get(String name, Class<T> clazz) {
        return ReactorNetty4BaseHttpChannel.get(request, name, clazz);
    }

    FullHttpResponse createResponse(HttpResponse response) {
        return (FullHttpResponse) response;
    }
}
