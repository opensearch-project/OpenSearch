/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.reactor.netty4;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.Booleans;
import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.util.NettyRuntime;

/**
 * Shameless copy of Netty4Utils from transport-netty4 module
 */
public final class Netty4Utils {
    private static final AtomicBoolean isAvailableProcessorsSet = new AtomicBoolean();

    /**
     * Utility class
     */
    private Netty4Utils() {}

    /**
     * Set the number of available processors that Netty uses for sizing various resources (e.g., thread pools).
     *
     * @param availableProcessors the number of available processors
     * @throws IllegalStateException if available processors was set previously and the specified value does not match the already-set value
     */
    public static void setAvailableProcessors(final int availableProcessors) {
        // we set this to false in tests to avoid tests that randomly set processors from stepping on each other
        final boolean set = Booleans.parseBoolean(System.getProperty("opensearch.set.netty.runtime.available.processors", "true"));
        if (!set) {
            return;
        }

        /*
         * This can be invoked twice, once from Netty4Transport and another time from Netty4HttpServerTransport; however,
         * Netty4Runtime#availableProcessors forbids settings the number of processors twice so we prevent double invocation here.
         */
        if (isAvailableProcessorsSet.compareAndSet(false, true)) {
            NettyRuntime.setAvailableProcessors(availableProcessors);
        } else if (availableProcessors != NettyRuntime.availableProcessors()) {
            /*
             * We have previously set the available processors yet either we are trying to set it to a different value now or there is a bug
             * in Netty and our previous value did not take, bail.
             */
            final String message = String.format(
                Locale.ROOT,
                "available processors value [%d] did not match current value [%d]",
                availableProcessors,
                NettyRuntime.availableProcessors()
            );
            throw new IllegalStateException(message);
        }
    }

    /**
     * Turns the given BytesReference into a ByteBuf. Note: the returned ByteBuf will reference the internal
     * pages of the BytesReference. Don't free the bytes of reference before the ByteBuf goes out of scope.
     * @param reference reference to convert
     */
    public static ByteBuf toByteBuf(final BytesReference reference) {
        if (reference.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        final BytesRefIterator iterator = reference.iterator();
        // usually we have one, two, or three components from the header, the message, and a buffer
        final List<ByteBuf> buffers = new ArrayList<>(3);
        try {
            BytesRef slice;
            while ((slice = iterator.next()) != null) {
                buffers.add(Unpooled.wrappedBuffer(slice.bytes, slice.offset, slice.length));
            }

            if (buffers.size() == 1) {
                return buffers.get(0);
            } else {
                CompositeByteBuf composite = Unpooled.compositeBuffer(buffers.size());
                composite.addComponents(true, buffers);
                return composite;
            }
        } catch (IOException ex) {
            throw new AssertionError("no IO happens here", ex);
        }
    }

    /**
     * Wraps the given ChannelBuffer with a BytesReference
     * @param buffer buffer to convert
     */
    public static BytesReference toBytesReference(final ByteBuf buffer) {
        final int readableBytes = buffer.readableBytes();
        if (readableBytes == 0) {
            return BytesArray.EMPTY;
        } else if (buffer.hasArray()) {
            return new BytesArray(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), readableBytes);
        } else {
            final ByteBuffer[] byteBuffers = buffer.nioBuffers();
            return BytesReference.fromByteBuffers(byteBuffers);
        }
    }

    /**
     * Add completion listener to ChannelFuture
     * @param channelFuture ChannelFuture to add listener to
     * @param context completion listener context
     */
    public static void addListener(ChannelFuture channelFuture, CompletableContext<Void> context) {
        channelFuture.addListener(f -> {
            if (f.isSuccess()) {
                context.complete(null);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    ExceptionsHelper.maybeDieOnAnotherThread(cause);
                    context.completeExceptionally(new Exception(cause));
                } else {
                    context.completeExceptionally((Exception) cause);
                }
            }
        });
    }
}
