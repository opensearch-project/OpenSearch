/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.core.action.ActionListener;

import io.netty.handler.codec.http.HttpContent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

class ReactorNetty4StreamingResponseProducer implements StreamingHttpContentSender, Publisher<HttpContent> {
    private final Publisher<HttpContent> sender;
    private volatile FluxSink<HttpContent> emitter;

    ReactorNetty4StreamingResponseProducer() {
        this.sender = Flux.create(emitter -> this.emitter = emitter);
    }

    @Override
    public void send(HttpContent content, ActionListener<Void> listener, boolean isLast) {
        try {
            emitter.next(content);
            listener.onResponse(null);
            if (isLast) {
                emitter.complete();
            }
        } catch (final Exception ex) {
            emitter.error(ex);
            listener.onFailure(ex);
        }
    }

    @Override
    public void subscribe(Subscriber<? super HttpContent> s) {
        sender.subscribe(s);
    }

    @Override
    public boolean isReady() {
        return emitter != null;
    }

    FluxSink<HttpContent> emitter() {
        return emitter;
    }
}
