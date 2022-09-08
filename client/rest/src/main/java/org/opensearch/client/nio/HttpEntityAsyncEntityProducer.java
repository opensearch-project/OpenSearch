/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.nio;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.DataStreamChannel;
import org.apache.hc.core5.util.Args;
import org.apache.hc.core5.util.Asserts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class HttpEntityAsyncEntityProducer implements AsyncEntityProducer {

    private final HttpEntity entity;
    private final ByteBuffer byteBuffer;
    private final boolean chunked;
    private final AtomicReference<Exception> exception;
    private final AtomicReference<ReadableByteChannel> channelRef;
    private boolean eof;

    public HttpEntityAsyncEntityProducer(final HttpEntity entity, final int bufferSize) {
        this.entity = Args.notNull(entity, "Http Entity");
        this.byteBuffer = ByteBuffer.allocate(bufferSize);
        this.chunked = entity.isChunked();
        this.exception = new AtomicReference<>();
        this.channelRef = new AtomicReference<>();
    }

    public HttpEntityAsyncEntityProducer(final HttpEntity entity) {
        this(entity, 8192);
    }

    @Override
    public boolean isRepeatable() {
        return entity.isRepeatable();
    }

    @Override
    public String getContentType() {
        return entity.getContentType();
    }

    @Override
    public long getContentLength() {
        return entity.getContentLength();
    }

    @Override
    public int available() {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getContentEncoding() {
        return entity.getContentEncoding();
    }

    @Override
    public boolean isChunked() {
        return chunked;
    }

    @Override
    public Set<String> getTrailerNames() {
        return entity.getTrailerNames();
    }

    @Override
    public void produce(final DataStreamChannel channel) throws IOException {
        ReadableByteChannel stream = channelRef.get();
        if (stream == null) {
            stream = Channels.newChannel(entity.getContent());
            Asserts.check(channelRef.getAndSet(stream) == null, "Illegal producer state");
        }
        if (!eof) {
            final int bytesRead = stream.read(byteBuffer);
            if (bytesRead < 0) {
                eof = true;
            }
        }
        if (byteBuffer.position() > 0) {
            byteBuffer.flip();
            channel.write(byteBuffer);
            byteBuffer.compact();
        }
        if (eof && byteBuffer.position() == 0) {
            channel.endStream();
            releaseResources();
        }
    }

    @Override
    public void failed(final Exception cause) {
        if (exception.compareAndSet(null, cause)) {
            releaseResources();
        }
    }

    public Exception getException() {
        return exception.get();
    }

    @Override
    public void releaseResources() {
        eof = false;
        channelRef.set(null);
    }

}
