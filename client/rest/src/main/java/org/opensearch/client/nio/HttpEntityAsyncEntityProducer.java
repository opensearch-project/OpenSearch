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
import org.apache.hc.core5.http.nio.ResourceHolder;
import org.apache.hc.core5.util.Args;
import org.apache.hc.core5.util.Asserts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link AsyncEntityProducer} implementation for {@link HttpEntity}
 */
public class HttpEntityAsyncEntityProducer implements AsyncEntityProducer {

    private final HttpEntity entity;
    private final ByteBuffer byteBuffer;
    private final boolean chunked;
    private final AtomicReference<Exception> exception;
    private final AtomicReference<ReadableByteChannel> channelRef;
    private boolean eof;

    /**
     * Create new async HTTP entity producer
     * @param entity HTTP entity
     * @param bufferSize buffer size
     */
    public HttpEntityAsyncEntityProducer(final HttpEntity entity, final int bufferSize) {
        this.entity = Args.notNull(entity, "Http Entity");
        this.byteBuffer = ByteBuffer.allocate(bufferSize);
        this.chunked = entity.isChunked();
        this.exception = new AtomicReference<>();
        this.channelRef = new AtomicReference<>();
    }

    /**
     * Create new async HTTP entity producer with default buffer size (8192 bytes)
     * @param entity HTTP entity
     */
    public HttpEntityAsyncEntityProducer(final HttpEntity entity) {
        this(entity, 8192);
    }

    /**
     * Determines whether the producer can consistently produce the same content
     * after invocation of {@link ResourceHolder#releaseResources()}.
     */
    @Override
    public boolean isRepeatable() {
        return entity.isRepeatable();
    }

    /**
     * Returns content type of the entity, if known.
     */
    @Override
    public String getContentType() {
        return entity.getContentType();
    }

    /**
     * Returns length of the entity, if known.
     */
    @Override
    public long getContentLength() {
        return entity.getContentLength();
    }

    /**
     * Returns the number of bytes immediately available for output.
     * This method can be used as a hint to control output events
     * of the underlying I/O session.
     *
     * @return the number of bytes immediately available for output
     */
    @Override
    public int available() {
        return Integer.MAX_VALUE;
    }

    /**
     * Returns content encoding of the entity, if known.
     */
    @Override
    public String getContentEncoding() {
        return entity.getContentEncoding();
    }

    /**
     * Returns chunked transfer hint for this entity.
     * <p>
     * The behavior of wrapping entities is implementation dependent,
     * but should respect the primary purpose.
     * </p>
     */
    @Override
    public boolean isChunked() {
        return chunked;
    }

    /**
     * Preliminary declaration of trailing headers.
     */
    @Override
    public Set<String> getTrailerNames() {
        return entity.getTrailerNames();
    }

    /**
     * Triggered to signal the ability of the underlying data channel
     * to accept more data. The data producer can choose to write data
     * immediately inside the call or asynchronously at some later point.
     *
     * @param channel the data channel capable to accepting more data.
     */
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

    /**
     * Triggered to signal a failure in data generation.
     *
     * @param cause the cause of the failure.
     */
    @Override
    public void failed(final Exception cause) {
        if (exception.compareAndSet(null, cause)) {
            releaseResources();
        }
    }

    /**
     * Release resources being held
     */
    @Override
    public void releaseResources() {
        eof = false;
        final ReadableByteChannel stream = channelRef.getAndSet(null);
        if (stream != null) {
            try {
                stream.close();
            } catch (final IOException ex) {
                /* Close quietly */
            }
        }
    }

}
