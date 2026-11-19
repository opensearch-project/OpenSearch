/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.stream;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.channel.CoalescingBufferQueue;
import io.grpc.netty.shaded.io.netty.channel.embedded.EmbeddedChannel;

public class ZeroCopyBufferChainTests extends OpenSearchTestCase {

    /**
     * Fails when a grpc-java upgrade renames the internals zero-copy writes depend on, instead of silently copying.
     */
    public void testGrpcInternalsAreAvailable() {
        assertTrue(ZeroCopyBufferChain.isAvailable());
    }

    public void testCompositeIsDirectSoNettyDoesNotCopyIt() {
        ByteBuffer body = ByteBuffer.allocateDirect(4).put(new byte[] { 5, 6, 7, 8 }).flip();
        List<ByteBuffer> segments = List.of(
            ByteBuffer.wrap(new byte[] { 1, 2 }),
            ByteBuffer.wrap(new byte[] { 3, 4 }),
            body,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[] { 9 })
        );
        ZeroCopyBufferChain.ReleasingCompositeByteBuf buffer = new ZeroCopyBufferChain.ReleasingCompositeByteBuf(segments);
        try {
            assertTrue(buffer.isDirect());
            // The two leading heap segments share one copy; the direct body is wrapped, not copied.
            assertEquals(3, buffer.numComponents());
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.getBytes(0, bytes);
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, bytes);
            assertEquals(0, body.position());
        } finally {
            AtomicBoolean released = new AtomicBoolean();
            buffer.arm(() -> released.set(true));
            buffer.release();
            assertTrue(released.get());
        }
    }

    /**
     * HTTP/2 flow control queues the writes of a stream and coalesces them into the first one, appending the later buffers
     * to it when it is composite. That must not make it consolidate, which copies the message into a new buffer.
     */
    public void testNettyCoalescesWritesBehindTheBufferWithoutCopyingIt() {
        ByteBuffer body = ByteBuffer.allocateDirect(4).put(new byte[] { 5, 6, 7, 8 }).flip();
        ZeroCopyBufferChain.ReleasingCompositeByteBuf first = new ZeroCopyBufferChain.ReleasingCompositeByteBuf(
            List.of(ByteBuffer.wrap(new byte[] { 1, 2 }), ByteBuffer.wrap(new byte[] { 3, 4 }), body)
        );
        AtomicBoolean firstReleased = new AtomicBoolean();
        first.arm(() -> firstReleased.set(true));
        ZeroCopyBufferChain.ReleasingCompositeByteBuf next = new ZeroCopyBufferChain.ReleasingCompositeByteBuf(
            List.of(ByteBuffer.wrap(new byte[] { 10 }), ByteBuffer.allocateDirect(2).put(new byte[] { 11, 12 }).flip())
        );
        AtomicBoolean nextReleased = new AtomicBoolean();
        next.arm(() -> nextReleased.set(true));

        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            CoalescingBufferQueue queue = new CoalescingBufferQueue(channel);
            // Partly written, so it stays first in the queue while the next message's header and payload queue behind it.
            first.skipBytes(1);
            queue.add(first);
            queue.add(Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 0, 3 }));
            queue.add(next);
            ByteBuf coalesced = queue.remove(queue.readableBytes(), channel.newPromise());
            try {
                // Still the caller's memory: a copy would not see this write.
                body.put(0, (byte) 50);
                byte[] bytes = new byte[coalesced.readableBytes()];
                coalesced.getBytes(coalesced.readerIndex(), bytes);
                assertArrayEquals(new byte[] { 2, 3, 4, 50, 6, 7, 8, 0, 0, 0, 0, 3, 10, 11, 12 }, bytes);
                assertFalse(firstReleased.get());
                assertFalse(nextReleased.get());
            } finally {
                coalesced.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
        assertTrue(firstReleased.get());
        assertTrue(nextReleased.get());
    }

    public void testDeclinesStreamsOtherThanTheGrpcBufferChain() {
        AtomicBoolean released = new AtomicBoolean();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertFalse(ZeroCopyBufferChain.tryAppend(out, List.of(ByteBuffer.wrap(new byte[] { 1 })), () -> released.set(true)));
        assertEquals(0, out.size());
        assertFalse(released.get());
    }
}
