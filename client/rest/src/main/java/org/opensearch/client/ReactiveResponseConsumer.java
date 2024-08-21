/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.BasicFuture;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.AsyncByteConsumer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Args;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.reactivestreams.Publisher;

class ReactiveResponseConsumer extends AsyncByteConsumer<HttpResponse> {
    private final BasicFuture<Message<HttpResponse, Publisher<ByteBuffer>>> responseFuture;
    private final ReactiveDataConsumer reactiveDataConsumer = new ReactiveDataConsumer();

    ReactiveResponseConsumer(final FutureCallback<Message<HttpResponse, Publisher<ByteBuffer>>> responseCallback) {
        this.responseFuture = new BasicFuture<>(Args.notNull(responseCallback, "responseCallback"));
    }

    @Override
    protected void onByteReceived(ByteBuffer buf, IOControl ioctrl) throws IOException {
        reactiveDataConsumer.consume(buf);
        ioctrl.requestInput();
    }

    @Override
    public void onResponseReceived(HttpResponse response) throws HttpException, IOException {
        responseFuture.completed(new Message<>(response, reactiveDataConsumer));
    }

    @Override
    public HttpResponse buildResult(HttpContext context) throws Exception {
        reactiveDataConsumer.complete();
        return null;
    }

    @Override
    protected void releaseResources() {
        if (getException() != null) {
            reactiveDataConsumer.failed(getException());
            responseFuture.failed(getException());
        }
    }
}
