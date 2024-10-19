/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.http.HttpChunk;
import org.opensearch.http.StreamingHttpChannel;

import java.util.function.Consumer;

import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

class ReactorNetty4StreamingRequestConsumer<T extends HttpContent> implements Consumer<T>, Publisher<HttpContent> {
    private final ReactorNetty4StreamingResponseProducer sender;
    private final StreamingHttpChannel httpChannel;

    ReactorNetty4StreamingRequestConsumer(HttpServerRequest request, HttpServerResponse response) {
        this.sender = new ReactorNetty4StreamingResponseProducer();
        this.httpChannel = new ReactorNetty4StreamingHttpChannel(request, response, sender);
    }

    @Override
    public void accept(T message) {
        if (message instanceof LastHttpContent) {
            httpChannel.receiveChunk(createChunk(message, true));
        } else if (message instanceof HttpContent) {
            httpChannel.receiveChunk(createChunk(message, false));
        }
    }

    @Override
    public void subscribe(Subscriber<? super HttpContent> s) {
        sender.subscribe(s);
    }

    HttpChunk createChunk(HttpContent chunk, boolean last) {
        return new ReactorNetty4HttpChunk(chunk.content(), last);
    }

    StreamingHttpChannel httpChannel() {
        return httpChannel;
    }
}
