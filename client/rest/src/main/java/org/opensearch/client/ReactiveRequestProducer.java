/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.reactivestreams.Publisher;

class ReactiveRequestProducer implements HttpAsyncRequestProducer {
    private final HttpHost target;
    private final HttpRequest request;
    private final ReactiveDataProducer producer;

    private static final class ReactiveHttpEntity implements HttpEntity {
        private final Header contentType;
        private final Header contentEncoding;

        private ReactiveHttpEntity(final Header contentType, final Header contentEncoding) {
            this.contentType = contentType;
            this.contentEncoding = contentEncoding;
        }

        @Override
        public void writeTo(OutputStream outStream) throws IOException {
            throw new UnsupportedOperationException("This operation is not supported");
        }

        @Override
        public boolean isStreaming() {
            return true;
        }

        @Override
        public boolean isRepeatable() {
            return false;
        }

        @Override
        public boolean isChunked() {
            return true;
        }

        @Override
        public Header getContentType() {
            return contentType;
        }

        @Override
        public long getContentLength() {
            return -1;
        }

        @Override
        public Header getContentEncoding() {
            return contentEncoding;
        }

        @Override
        public InputStream getContent() throws IOException, UnsupportedOperationException {
            throw new UnsupportedOperationException("This operation is not supported");
        }

        @Override
        public void consumeContent() throws IOException {
            throw new UnsupportedOperationException("This operation is not supported");
        }
    };

    ReactiveRequestProducer(HttpRequest request, HttpHost target, Publisher<ByteBuffer> publisher) {
        this.target = target;
        this.request = request;
        this.producer = new ReactiveDataProducer(publisher);
    }

    @Override
    public HttpRequest generateRequest() {
        final Header contentTypeHeader = request.getFirstHeader("Content-Type");
        if (contentTypeHeader == null) {
            request.setHeader(new BasicHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType()));
        }

        if (request instanceof HttpEntityEnclosingRequest) {
            final HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;
            enclosingRequest.setEntity(
                new ReactiveHttpEntity(request.getFirstHeader("Content-Type"), request.getFirstHeader("Content-Encoding"))
            );
        }

        return this.request;
    }

    @Override
    public HttpHost getTarget() {
        return this.target;
    }

    @Override
    public void produceContent(final ContentEncoder encoder, final IOControl ioControl) throws IOException {
        if (this.producer != null) {
            this.producer.produceContent(encoder, ioControl);
            if (encoder.isCompleted()) {
                this.producer.close();
            }
        }
    }

    @Override
    public void requestCompleted(final HttpContext context) {
        this.producer.onComplete();
    }

    @Override
    public void failed(final Exception ex) {
        this.producer.onError(ex);
    }

    @Override
    public boolean isRepeatable() {
        return this.producer.isRepeatable();
    }

    @Override
    public void resetRequest() throws IOException {
        this.producer.close();
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(this.target);
        sb.append(' ');
        sb.append(this.request);
        if (this.producer != null) {
            sb.append(' ');
            sb.append(this.producer);
        }
        return sb.toString();
    }

}
