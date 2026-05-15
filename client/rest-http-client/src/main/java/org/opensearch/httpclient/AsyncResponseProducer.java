/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.httpclient;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

class AsyncResponseProducer implements Subscriber<List<ByteBuffer>> {
    private Subscription subscription;
    private final List<ByteBuffer> buffers = new Vector<>();

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(List<ByteBuffer> item) {
        buffers.addAll(item);
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void onComplete() {}

    public List<ByteBuffer> getResult() {
        return buffers;
    }
}
