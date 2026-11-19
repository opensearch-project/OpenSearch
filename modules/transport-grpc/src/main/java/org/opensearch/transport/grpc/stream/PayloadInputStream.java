/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.stream;

import org.opensearch.transport.grpc.spi.StreamPayload;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.Drainable;

/**
 * The bytes of one gRPC message: a protobuf field prefix followed by a {@link StreamPayload}.
 * <p>
 * The stream is deliberately not {@code KnownLength}, so gRPC drains it into its buffer chain, which
 * {@link ZeroCopyBufferChain} extends without copying. When that hand-off is unavailable, or when the stream is read, the
 * bytes are copied. The payload is released exactly once: by Netty after a zero-copy hand-off, after a copying drain, or at
 * the latest by {@link #close()}, which gRPC always calls.
 */
final class PayloadInputStream extends InputStream implements Drainable {

    private static final int COPY_CHUNK_SIZE = 8192;

    private final StreamPayload payload;
    private final List<ByteBuffer> segments;
    private final int length;
    private final AtomicBoolean consumed = new AtomicBoolean();
    private final AtomicBoolean transferred = new AtomicBoolean();
    private ByteBuffer[] readViews;
    private int readSegment;

    PayloadInputStream(ByteBuffer prefix, StreamPayload payload) {
        final long total = prefix.remaining() + payload.length();
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("payload of [" + payload.length() + "] bytes exceeds the maximum gRPC message size");
        }
        this.payload = payload;
        this.length = (int) total;
        final List<ByteBuffer> all = new ArrayList<>(payload.segments().size() + 1);
        all.add(prefix);
        all.addAll(payload.segments());
        this.segments = List.copyOf(all);
    }

    @Override
    public int drainTo(OutputStream target) throws IOException {
        markConsumed();
        if (ZeroCopyBufferChain.tryAppend(target, segments, payload::release)) {
            transferred.set(true);
            return length;
        }
        try {
            final byte[] chunk = new byte[Math.max(1, Math.min(COPY_CHUNK_SIZE, length))];
            for (ByteBuffer segment : segments) {
                final ByteBuffer source = segment.duplicate();
                if (source.hasArray()) {
                    target.write(source.array(), source.arrayOffset() + source.position(), source.remaining());
                    continue;
                }
                while (source.hasRemaining()) {
                    final int n = Math.min(chunk.length, source.remaining());
                    source.get(chunk, 0, n);
                    target.write(chunk, 0, n);
                }
            }
        } finally {
            payload.release();
        }
        return length;
    }

    @Override
    public int read() {
        final ByteBuffer segment = nextReadable();
        return segment == null ? -1 : segment.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (len == 0) {
            return 0;
        }
        int read = 0;
        ByteBuffer segment;
        while (read < len && (segment = nextReadable()) != null) {
            final int n = Math.min(len - read, segment.remaining());
            segment.get(b, off + read, n);
            read += n;
        }
        return read == 0 ? -1 : read;
    }

    private ByteBuffer nextReadable() {
        if (readViews == null) {
            markConsumed();
            // Private views keep the positions of the caller's buffers untouched.
            readViews = segments.stream().map(ByteBuffer::duplicate).toArray(ByteBuffer[]::new);
        }
        if (payload.isReleased()) {
            throw new IllegalStateException("payload was released");
        }
        while (readSegment < readViews.length) {
            if (readViews[readSegment].hasRemaining()) {
                return readViews[readSegment];
            }
            readSegment++;
        }
        return null;
    }

    private void markConsumed() {
        if (consumed.compareAndSet(false, true) == false) {
            throw new IllegalStateException("payload was already consumed");
        }
    }

    /**
     * Releases the payload unless its memory was handed to the transport.
     */
    @Override
    public void close() {
        consumed.set(true);
        if (transferred.get() == false) {
            payload.release();
        }
    }

    /**
     * Returns the number of bytes in the message.
     */
    int length() {
        return length;
    }
}
