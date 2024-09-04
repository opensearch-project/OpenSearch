/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.entity.HttpAsyncContentProducer;
import org.apache.http.util.Args;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Adapted from https://github.com/apache/httpcomponents-core/blob/master/httpcore5-reactive/src/main/java/org/apache/hc/core5/reactive/ReactiveDataProducer.java
 */
class ReactiveDataProducer implements HttpAsyncContentProducer, Subscriber<ByteBuffer> {
    private static final int BUFFER_WINDOW_SIZE = 5;
    private final AtomicReference<IOControl> controlChannel = new AtomicReference<>();
    private final AtomicReference<Throwable> exception = new AtomicReference<>();
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final Publisher<ByteBuffer> publisher;
    private final AtomicReference<Subscription> subscription = new AtomicReference<>();
    private final ArrayDeque<ByteBuffer> buffers = new ArrayDeque<>(); // This field requires synchronization
    private final ReentrantLock lock;

    public ReactiveDataProducer(final Publisher<ByteBuffer> publisher) {
        this.publisher = Args.notNull(publisher, "publisher");
        this.lock = new ReentrantLock();
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
        if (this.subscription.getAndSet(subscription) != null) {
            throw new IllegalStateException("Already subscribed");
        }

        subscription.request(BUFFER_WINDOW_SIZE);
    }

    @Override
    public void onNext(final ByteBuffer byteBuffer) {
        final byte[] copy = new byte[byteBuffer.remaining()];
        byteBuffer.get(copy);

        lock.lock();
        try {
            buffers.add(ByteBuffer.wrap(copy));
        } finally {
            lock.unlock();
        }

        if (controlChannel.get() != null) {
            controlChannel.get().requestOutput();
        }
    }

    @Override
    public void onError(final Throwable throwable) {
        subscription.set(null);
        exception.set(throwable);
        if (controlChannel.get() != null) {
            controlChannel.get().requestOutput();
        }
    }

    @Override
    public void onComplete() {
        subscription.set(null);
        complete.set(true);
        if (controlChannel.get() != null) {
            controlChannel.get().requestOutput();
        }
    }

    @Override
    public void produceContent(ContentEncoder encoder, IOControl ioControl) throws IOException {
        if (controlChannel.get() == null) {
            controlChannel.set(ioControl);
            publisher.subscribe(this);
        }

        final Throwable t = exception.get();
        final Subscription s = subscription.get();
        int buffersToReplenish = 0;
        try {
            lock.lock();
            try {
                if (t != null) {
                    throw new IOException(t.getMessage(), t);
                } else if (this.complete.get() && buffers.isEmpty()) {
                    encoder.complete();
                } else {
                    while (!buffers.isEmpty()) {
                        final ByteBuffer nextBuffer = buffers.remove();
                        encoder.write(nextBuffer);
                        if (nextBuffer.remaining() > 0) {
                            buffers.push(nextBuffer);
                            break;
                        } else if (s != null) {
                            // We defer the #request call until after we release the buffer lock.
                            buffersToReplenish++;
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        } finally {
            if (s != null && buffersToReplenish > 0) {
                s.request(buffersToReplenish);
            }

            if (!this.complete.get()) {
                ioControl.suspendOutput();
            }
        }
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public void close() throws IOException {
        controlChannel.set(null);

        final Subscription s = subscription.getAndSet(null);
        if (s != null) {
            s.cancel();
        }
    }
}
