/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.CompositeByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.buffer.UnpooledByteBufAllocator;

/**
 * Hands memory to gRPC's outbound path without copying it.
 * <p>
 * gRPC drains a message stream that is {@code Drainable} but not {@code KnownLength} into a
 * {@code MessageFramer$BufferChainOutputStream}, which accumulates the message as a list of {@code WritableBuffer}s and
 * then passes each one to Netty. Appending a {@code NettyWritableBuffer} that wraps the caller's memory to that list makes
 * Netty write the memory directly to the socket. Arrow Flight uses the same technique
 * ({@code org.apache.arrow.flight.grpc.AddWritableBuffer}); this variant binds to the classes of grpc-netty-shaded.
 * <p>
 * Both classes are grpc-java internals. The lookup is verified once, and {@link #tryAppend} returns {@code false} when it
 * is unavailable; callers then copy.
 */
final class ZeroCopyBufferChain {

    private static final Logger logger = LogManager.getLogger(ZeroCopyBufferChain.class);

    static final String BUFFER_CHAIN_CLASS = "io.grpc.internal.MessageFramer$BufferChainOutputStream";
    static final String NETTY_WRITABLE_BUFFER_CLASS = "io.grpc.netty.shaded.io.grpc.netty.NettyWritableBuffer";

    private static final Internals INTERNALS = Internals.lookup();

    private ZeroCopyBufferChain() {}

    /**
     * Returns whether the grpc-java internals that this class depends on were found.
     */
    static boolean isAvailable() {
        return INTERNALS != null;
    }

    /**
     * Appends {@code segments}, in order, to {@code target} as a single buffer that Netty writes without copying.
     *
     * @param target    the stream gRPC passed to {@code Drainable#drainTo}
     * @param segments  the memory to append; must stay valid until {@code onRelease} runs
     * @param onRelease run once, on a Netty thread, after Netty no longer references the memory
     * @return {@code true} if appended, which makes Netty responsible for running {@code onRelease}; {@code false} if the
     *         internals are unavailable or {@code target} is not gRPC's buffer chain, for example because compression is
     *         enabled, in which case nothing changes and {@code onRelease} does not run
     */
    static boolean tryAppend(OutputStream target, List<ByteBuffer> segments, Runnable onRelease) {
        final Internals internals = INTERNALS;
        if (internals == null || target.getClass() != internals.bufferChainClass) {
            return false;
        }
        final ReleasingCompositeByteBuf buffer = new ReleasingCompositeByteBuf(segments);
        try {
            internals.append(target, buffer);
        } catch (ReflectiveOperationException | RuntimeException e) {
            // The buffer is not armed yet, so releasing it frees only Netty's buffers. The copy fallback still reads the
            // caller's memory.
            buffer.release();
            logger.warn("failed to append a buffer to the gRPC message, copying instead", e);
            return false;
        }
        // gRPC hands the buffer to Netty only after drainTo returns, so arming here cannot miss the release.
        buffer.arm(onRelease);
        return true;
    }

    private record Internals(Class<?> bufferChainClass, Field bufferList, Field current, Constructor<?> writableBufferConstructor) {

        @SuppressForbidden(reason = "binds to grpc-java internals to hand off memory without copying")
        static Internals lookup() {
            try {
                final ClassLoader loader = ZeroCopyBufferChain.class.getClassLoader();
                final Class<?> bufferChainClass = Class.forName(BUFFER_CHAIN_CLASS, false, loader);
                final Field bufferList = bufferChainClass.getDeclaredField("bufferList");
                final Field current = bufferChainClass.getDeclaredField("current");
                final Class<?> writableBufferClass = Class.forName(NETTY_WRITABLE_BUFFER_CLASS, false, loader);
                final Constructor<?> writableBufferConstructor = writableBufferClass.getDeclaredConstructor(ByteBuf.class);
                if (List.class.isAssignableFrom(bufferList.getType()) == false
                    || current.getType().isAssignableFrom(writableBufferClass) == false) {
                    throw new NoSuchFieldException("unexpected field types in " + BUFFER_CHAIN_CLASS);
                }
                bufferList.setAccessible(true);
                current.setAccessible(true);
                writableBufferConstructor.setAccessible(true);
                return new Internals(bufferChainClass, bufferList, current, writableBufferConstructor);
            } catch (ReflectiveOperationException | RuntimeException e) {
                logger.warn("gRPC zero-copy writes are unavailable; streamed payloads are copied", e);
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        void append(OutputStream target, ByteBuf buffer) throws ReflectiveOperationException {
            final Object writableBuffer = writableBufferConstructor.newInstance(buffer);
            final List<Object> buffers = (List<Object>) bufferList.get(target);
            // Making the new buffer current makes later writes allocate a fresh buffer after it instead of writing into an
            // earlier one, which keeps the bytes in order. The new buffer is full, so no write goes into it.
            current.set(target, writableBuffer);
            buffers.add(writableBuffer);
        }
    }

    /**
     * The segments of a message as one buffer that runs a callback when Netty frees it. Netty copies a buffer that is not
     * entirely direct into direct memory before writing it. This buffer therefore copies each run of heap segments, such as
     * small headers, into pooled direct memory, and wraps the large direct segments as they are.
     */
    static final class ReleasingCompositeByteBuf extends CompositeByteBuf {

        private volatile Runnable onDeallocate;

        ReleasingCompositeByteBuf(List<ByteBuffer> segments) {
            // No component limit: when Netty coalesces the writes queued behind this buffer, it appends them to it. Netty
            // consolidates a composite over its limit, which copies.
            super(UnpooledByteBufAllocator.DEFAULT, true, Integer.MAX_VALUE);
            int i = 0;
            while (i < segments.size()) {
                final ByteBuffer segment = segments.get(i);
                if (segment.isDirect()) {
                    if (segment.hasRemaining()) {
                        addComponent(true, Unpooled.wrappedBuffer(segment));
                    }
                    i++;
                    continue;
                }
                int end = i;
                int heapBytes = 0;
                while (end < segments.size() && segments.get(end).isDirect() == false) {
                    heapBytes += segments.get(end).remaining();
                    end++;
                }
                if (heapBytes > 0) {
                    final ByteBuf copy = PooledByteBufAllocator.DEFAULT.directBuffer(heapBytes, heapBytes);
                    for (int j = i; j < end; j++) {
                        copy.writeBytes(segments.get(j).duplicate());
                    }
                    addComponent(true, copy);
                }
                i = end;
            }
        }

        /** Sets the callback that runs once Netty frees this buffer. */
        void arm(Runnable onDeallocate) {
            this.onDeallocate = onDeallocate;
        }

        @Override
        protected void deallocate() {
            try {
                super.deallocate();
            } finally {
                final Runnable callback = onDeallocate;
                if (callback != null) {
                    callback.run();
                }
            }
        }
    }
}
