/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.http.AbstractHttpServerTransport;
import org.opensearch.http.HttpRequest;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.netty.buffer.CompositeByteBuf;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

class NonStreamingRequestConsumer<T extends HttpContent> implements Consumer<T>, Publisher<HttpContent>, Disposable {
    private final HttpServerRequest request;
    private final HttpServerResponse response;
    private final CompositeByteBuf content;
    private final Publisher<HttpContent> publisher;
    private final AbstractHttpServerTransport transport;
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private volatile FluxSink<HttpContent> emitter;

    NonStreamingRequestConsumer(
        AbstractHttpServerTransport transport,
        HttpServerRequest request,
        HttpServerResponse response,
        int maxCompositeBufferComponents
    ) {
        this.transport = transport;
        this.request = request;
        this.response = response;
        this.content = response.alloc().compositeBuffer(maxCompositeBufferComponents);
        this.publisher = Flux.create(emitter -> register(emitter));
    }

    private void register(FluxSink<HttpContent> emitter) {
        this.emitter = emitter.onDispose(this).onCancel(this);
    }

    @Override
    public void accept(T message) {
        try {
            if (message instanceof LastHttpContent) {
                process(message, emitter);
            } else if (message instanceof HttpContent) {
                process(message, emitter);
            }
        } catch (Throwable ex) {
            emitter.error(ex);
        }
    }

    public void process(HttpContent in, FluxSink<HttpContent> emitter) {
        // Consume request body in full before dispatching it
        content.addComponent(true, in.content().retain());

        if (in instanceof LastHttpContent) {
            final NonStreamingHttpChannel channel = new NonStreamingHttpChannel(request, response, emitter);
            final HttpRequest r = createRequest(request, content);

            try {
                transport.incomingRequest(r, channel);
            } catch (Exception ex) {
                emitter.error(ex);
                transport.onException(channel, ex);
            } finally {
                r.release();
                if (disposed.compareAndSet(false, true)) {
                    this.content.release();
                }
            }
        }
    }

    HttpRequest createRequest(HttpServerRequest request, CompositeByteBuf content) {
        return new ReactorNetty4HttpRequest(request, content.retain());
    }

    @Override
    public void subscribe(Subscriber<? super HttpContent> s) {
        publisher.subscribe(s);
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            this.content.release();
        }
    }
}
