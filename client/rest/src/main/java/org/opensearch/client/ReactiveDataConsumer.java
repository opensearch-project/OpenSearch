/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.http.util.Args;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Adapted from https://github.com/apache/httpcomponents-core/blob/master/httpcore5-reactive/src/main/java/org/apache/hc/core5/reactive/ReactiveDataConsumer.java
 */
class ReactiveDataConsumer implements Publisher<ByteBuffer> {

    private final AtomicLong requests = new AtomicLong(0);

    private final BlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<>();
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);
    private final AtomicInteger windowScalingIncrement = new AtomicInteger(0);
    private volatile boolean completed;
    private volatile Exception exception;
    private volatile Subscriber<? super ByteBuffer> subscriber;

    private final ReentrantLock lock = new ReentrantLock();

    public void failed(final Exception cause) {
        if (!completed) {
            exception = cause;
            flushToSubscriber();
        }
    }

    public void consume(final ByteBuffer byteBuffer) throws IOException {
        if (completed) {
            throw new IllegalStateException("Received data past end of stream");
        }

        final byte[] copy = new byte[byteBuffer.remaining()];
        byteBuffer.get(copy);
        buffers.add(ByteBuffer.wrap(copy));

        flushToSubscriber();
    }

    public void complete() {
        completed = true;
        flushToSubscriber();
    }

    private void flushToSubscriber() {
        lock.lock();
        try {
            final Subscriber<? super ByteBuffer> s = subscriber;
            if (flushInProgress.getAndSet(true)) {
                return;
            }
            try {
                if (s == null) {
                    return;
                }
                if (exception != null) {
                    subscriber = null;
                    s.onError(exception);
                    return;
                }
                ByteBuffer next;
                while (requests.get() > 0 && ((next = buffers.poll()) != null)) {
                    final int bytesFreed = next.remaining();
                    s.onNext(next);
                    requests.decrementAndGet();
                    windowScalingIncrement.addAndGet(bytesFreed);
                }
                if (completed && buffers.isEmpty()) {
                    subscriber = null;
                    s.onComplete();
                }
            } finally {
                flushInProgress.set(false);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void subscribe(final Subscriber<? super ByteBuffer> subscriber) {
        this.subscriber = Args.notNull(subscriber, "subscriber");
        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(final long increment) {
                if (increment <= 0) {
                    failed(new IllegalArgumentException("The number of elements requested must be strictly positive"));
                    return;
                }
                requests.addAndGet(increment);
                flushToSubscriber();
            }

            @Override
            public void cancel() {
                ReactiveDataConsumer.this.subscriber = null;
            }
        });
    }

}
