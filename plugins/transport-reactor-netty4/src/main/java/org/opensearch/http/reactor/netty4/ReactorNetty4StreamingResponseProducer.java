/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.core.action.ActionListener;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.netty.handler.codec.http.HttpContent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

class ReactorNetty4StreamingResponseProducer implements StreamingHttpContentSender, Publisher<HttpContent> {
    // Buffer up to 64 messages, otherwise fail with buffer overflow (IllegalStateException)
    private static final int BUFFERED_QUEUE_SIZE = 64;

    private final Publisher<HttpContent> sender;
    private volatile FluxSink<HttpContent> emitter;
    // We choose unbounded queue as the safe buffer here (since it does not support capacity bounds),
    // realistically we should only see a single final (last content) response being deferred. The BUFFERED_QUEUE_SIZE
    // check takes care of potential overflows.
    private final Queue<DelayedHttpContent> queue = new ConcurrentLinkedQueue<>();

    // Holds the {@code HttpContent} for deferred delivery
    private record DelayedHttpContent(HttpContent content, ActionListener<Void> listener, boolean isLast) {
    };

    ReactorNetty4StreamingResponseProducer() {
        this.sender = Flux.create(emitter -> register(emitter));
    }

    private void register(FluxSink<HttpContent> emitter) {
        this.emitter = emitter;
    }

    @Override
    public void send(HttpContent content, ActionListener<Void> listener, boolean isLast) {
        // In case when the exception triggers the response **before** the emitter is being
        // created, we defer the send till the subscribe call happens.
        if (isReady() == false) {
            queue.offer(new DelayedHttpContent(content, listener, isLast));
            if (queue.size() > BUFFERED_QUEUE_SIZE) {
                final IllegalStateException ex = new IllegalStateException(
                    "The buffered queue size limit is exceeded: " + BUFFERED_QUEUE_SIZE
                );
                listener.onFailure(ex);
                throw ex;
            }
            return;
        }

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

        DelayedHttpContent content = null;
        while ((content = queue.poll()) != null) {
            send(content.content(), content.listener(), content.isLast());
        }
    }

    @Override
    public boolean isReady() {
        return emitter != null;
    }

    FluxSink<HttpContent> emitter() {
        return emitter;
    }
}
