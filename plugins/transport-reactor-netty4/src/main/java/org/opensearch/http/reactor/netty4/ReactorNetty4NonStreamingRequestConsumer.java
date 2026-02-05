/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

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

class ReactorNetty4NonStreamingRequestConsumer<T extends HttpContent> implements Consumer<T>, Publisher<HttpContent>, Disposable {
    private final HttpServerRequest request;
    private final HttpServerResponse response;
    private final CompositeByteBuf content;
    private final Publisher<HttpContent> publisher;
    private final ReactorNetty4HttpServerTransport transport;
    private final HttpResponseHeadersFactory responseHeadersFactory;
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private volatile FluxSink<HttpContent> emitter;
    private volatile boolean lastHttpContentEmitted = false;

    ReactorNetty4NonStreamingRequestConsumer(
        ReactorNetty4HttpServerTransport transport,
        HttpResponseHeadersFactory responseHeadersFactory,
        HttpServerRequest request,
        HttpServerResponse response,
        int maxCompositeBufferComponents
    ) {
        this.transport = transport;
        this.responseHeadersFactory = responseHeadersFactory;
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
        // In some cases, we may not see the LastHttpContent message from
        // Reactor Netty implementation as it may just complete the subscription
        // explicitly upon receiving DefaultLastHttpContent.EMPTY_LAST_CONTENT from Netty
        // (for example, Http3FrameToHttpObjectCodec does that). As such, we always
        // inject DefaultLastHttpContent.EMPTY_LAST_CONTENT at the end, however it could
        // lead to cases when consumer may see more than one LastHttpContent message.
        if (lastHttpContentEmitted == true) {
            return;
        }

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

    void process(HttpContent in, FluxSink<HttpContent> emitter) {
        // Consume request body in full before dispatching it
        content.addComponent(true, in.content().retain());

        if (in instanceof LastHttpContent) {
            lastHttpContentEmitted = true;

            final ReactorNetty4NonStreamingHttpChannel channel = new ReactorNetty4NonStreamingHttpChannel(request, response, emitter);
            final HttpRequest r = createRequest(request, content);

            try {
                transport.serverAcceptedChannel(channel);
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
        return new ReactorNetty4HttpRequest(request, content.retain(), responseHeadersFactory);
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
