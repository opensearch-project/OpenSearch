/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
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
    private final long maxContentLength;
    private volatile FluxSink<HttpContent> emitter;
    private volatile boolean lastHttpContentEmitted = false;
    private volatile HttpChannel channel;

    ReactorNetty4NonStreamingRequestConsumer(
        ReactorNetty4HttpServerTransport transport,
        HttpResponseHeadersFactory responseHeadersFactory,
        HttpServerRequest request,
        HttpServerResponse response,
        int maxCompositeBufferComponents,
        long maxContentLength
    ) {
        this.transport = transport;
        this.responseHeadersFactory = responseHeadersFactory;
        this.request = request;
        this.response = response;
        this.maxContentLength = maxContentLength;
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
        final ByteBuf chunk = in.content();

        // Enforce http.max_content_length on the received content (already decompressed if the request
        // body is compressed) before retaining it, the same way the netty4 transport does. As with the netty4
        // transport, the decoded chunk that crosses the limit has already been allocated by the decompressor
        // and is released right away: Netty's decoders emit at most 64 KiB per chunk with the default
        // http.max_chunk_size and HTTP/2 frame size, but a gzip/deflate chunk can reach about twice the
        // compressed body for a cleartext h2c upgrade request, which is aggregated before it is decoded.
        if ((long) content.readableBytes() + chunk.readableBytes() > maxContentLength) {
            rejectContentTooLarge(emitter);
            return;
        }

        // Consume request body in full before dispatching it
        content.addComponent(true, chunk.retain());

        if (in instanceof LastHttpContent) {
            lastHttpContentEmitted = true;

            channel = transport.newNonStreamingHttpChannel(request, response, emitter);
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

    private void rejectContentTooLarge(FluxSink<HttpContent> emitter) {
        // Ignore any further content of this request and release what has been buffered so far
        lastHttpContentEmitted = true;
        if (disposed.compareAndSet(false, true)) {
            this.content.release();
        }

        // The rest of the request body is not going to be consumed, so the connection cannot be reused
        response.keepAlive(false);

        final FullHttpResponse tooLarge = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
            Unpooled.EMPTY_BUFFER
        );
        HttpUtil.setContentLength(tooLarge, 0);
        emitter.next(tooLarge);
        emitter.complete();
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
        // Always close the HttpChannel instance here since the underlying publisher is disposed.
        final HttpChannel httpChannel = channel;
        if (httpChannel != null) {
            httpChannel.close();
            channel = null;
        }

        if (disposed.compareAndSet(false, true)) {
            this.content.release();
        }
    }
}
