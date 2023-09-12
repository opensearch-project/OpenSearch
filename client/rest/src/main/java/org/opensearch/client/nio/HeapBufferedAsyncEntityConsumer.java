/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client.nio;

import org.apache.hc.core5.http.ContentTooLongException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.nio.AsyncEntityConsumer;
import org.apache.hc.core5.http.nio.entity.AbstractBinAsyncEntityConsumer;
import org.apache.hc.core5.util.ByteArrayBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of {@link AsyncEntityConsumer}. Buffers the whole
 * response content in heap memory, meaning that the size of the buffer is equal to the content-length of the response.
 * Limits the size of responses that can be read based on a configurable argument. Throws an exception in case the entity is longer
 * than the configured buffer limit.
 */
public class HeapBufferedAsyncEntityConsumer extends AbstractBinAsyncEntityConsumer<byte[]> {

    private final int bufferLimitBytes;
    private AtomicReference<ByteArrayBuffer> bufferRef = new AtomicReference<>();

    /**
     * Creates a new instance of this consumer with the provided buffer limit.
     *
     * @param bufferLimit the buffer limit. Must be greater than 0.
     * @throws IllegalArgumentException if {@code bufferLimit} is less than or equal to 0.
     */
    public HeapBufferedAsyncEntityConsumer(int bufferLimit) {
        if (bufferLimit <= 0) {
            throw new IllegalArgumentException("bufferLimit must be greater than 0");
        }
        this.bufferLimitBytes = bufferLimit;
    }

    /**
     * Get the limit of the buffer.
     */
    public int getBufferLimit() {
        return bufferLimitBytes;
    }

    /**
     * Triggered to signal beginning of entity content stream.
     *
     * @param contentType the entity content type
     */
    @Override
    protected void streamStart(final ContentType contentType) throws HttpException, IOException {}

    /**
     * Triggered to obtain the capacity increment.
     *
     * @return the number of bytes this consumer is prepared to process.
     */
    @Override
    protected int capacityIncrement() {
        return Integer.MAX_VALUE;
    }

    /**
     * Triggered to pass incoming data packet to the data consumer.
     *
     * @param src the data packet.
     * @param endOfStream flag indicating whether this data packet is the last in the data stream.
     *
     */
    @Override
    protected void data(final ByteBuffer src, final boolean endOfStream) throws IOException {
        if (src == null) {
            return;
        }

        int len = src.limit();
        if (len < 0) {
            len = 4096;
        } else if (len > bufferLimitBytes) {
            throw new ContentTooLongException(
                "entity content is too long [" + len + "] for the configured buffer limit [" + bufferLimitBytes + "]"
            );
        }

        ByteArrayBuffer buffer = bufferRef.get();
        if (buffer == null) {
            buffer = new ByteArrayBuffer(len);
            if (bufferRef.compareAndSet(null, buffer) == false) {
                buffer = bufferRef.get();
            }
        }

        if (buffer.length() + len > bufferLimitBytes) {
            throw new ContentTooLongException(
                "entity content is too long [" + len + "] for the configured buffer limit [" + bufferLimitBytes + "]"
            );
        }

        if (src.hasArray()) {
            buffer.append(src.array(), src.arrayOffset() + src.position(), src.remaining());
        } else {
            while (src.hasRemaining()) {
                buffer.append(src.get());
            }
        }
    }

    /**
     * Triggered to generate entity representation.
     *
     * @return the entity content
     */
    @Override
    protected byte[] generateContent() throws IOException {
        final ByteArrayBuffer buffer = bufferRef.get();
        return buffer == null ? new byte[0] : buffer.toByteArray();
    }

    /**
     * Release resources being held
     */
    @Override
    public void releaseResources() {
        ByteArrayBuffer buffer = bufferRef.getAndSet(null);
        if (buffer != null) {
            buffer.clear();
            buffer = null;
        }
    }

    /**
     * Gets current byte buffer instance
     * @return byte buffer instance
     */
    ByteArrayBuffer getBuffer() {
        return bufferRef.get();
    }
}
