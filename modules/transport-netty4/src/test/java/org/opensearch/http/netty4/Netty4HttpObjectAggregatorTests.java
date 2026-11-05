/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Netty4HttpObjectAggregatorTests extends OpenSearchTestCase {
    private static final String GET = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
    private static final String OVERSIZED = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 17\r\n\r\n" + "x".repeat(17);

    public void testNormalRequestsDoNotInstallRejectionHandler() {
        try (Fixture fixture = new Fixture()) {
            fixture.receive(GET);
            assertNull(fixture.rejection());
            FullHttpRequest request = fixture.channel.readInbound();
            assertNotNull(request);
            request.release();
        }
    }

    public void testTerminalRejectionDiscardsAlreadyDecodedPipelinedRequest() {
        try (Fixture fixture = new Fixture()) {
            // One decoder input contains a complete oversized request and a subsequent valid request.
            fixture.receive(
                "POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n11\r\n" + "x".repeat(17) + "\r\n0\r\n\r\n" + GET
            );
            assertNotNull(fixture.rejection());
            assertTrue(fixture.rejection().isClosing());
            assertNull("No pipelined request may reach dispatch after a terminal rejection", fixture.channel.readInbound());
            assertEquals(1, fixture.writer.pending.size());
            fixture.writer.release();
            fixture.assertRejection();
        }
    }

    public void testGzipFailureInSameReadCannotOvertakeRejection() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.channel.pipeline()
                .addAfter(
                    fixture.channel.pipeline().context(HttpRequestDecoder.class).name(),
                    "decompressor",
                    new HttpContentDecompressor(0)
                );
            AtomicInteger errors = new AtomicInteger();
            fixture.channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                    errors.incrementAndGet();
                    ctx.close();
                }
            });
            byte[] gzip = Netty4HttpRequestSizeTestClient.encode(new byte[1024], "gzip");
            gzip[gzip.length - 8] ^= 1;
            // One decoder input contains the oversized compressed payload and its invalid checksum.
            fixture.channel.writeInbound(Unpooled.wrappedBuffer(Netty4HttpRequestSizeTestClient.request(gzip, "gzip", false, false)));
            assertEquals("The real gzip decoder must fail before releasing the response", 1, errors.get());
            assertEquals(1, fixture.writer.pending.size());
            assertTrue(fixture.channel.isOpen());
            assertTrue(fixture.rejection().isClosing());
            fixture.writer.release();
            fixture.assertRejection();
            assertFalse(fixture.channel.isOpen());
        }
    }

    public void testBothSizeLimitsInOneReadWriteOnlyOneRejection() throws Exception {
        try (Fixture fixture = new Fixture(false, 64)) {
            Netty4HttpObjectAggregator aggregator = fixture.channel.pipeline().get(Netty4HttpObjectAggregator.class);
            fixture.channel.pipeline()
                .addAfter(
                    fixture.channel.pipeline().context(HttpRequestDecoder.class).name(),
                    "encoded_size",
                    new Netty4HttpEncodedSizeHandler(64, ctx -> aggregator.reject(ctx.pipeline().context(aggregator), true))
                );
            fixture.channel.pipeline().addAfter("encoded_size", "decompressor", new HttpContentDecompressor(0));
            byte[] gzip = Netty4HttpRequestSizeTestClient.encode(new byte[1024], "gzip");
            assertTrue(gzip.length < 64);
            // Decoded content crosses first; subsequent encoded bytes from this same decoder input cross the other limit.
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            body.write(gzip);
            body.write(Netty4HttpRequestSizeTestClient.encode(new byte[0], "gzip"));
            body.write(Netty4HttpRequestSizeTestClient.encode(new byte[0], "gzip"));
            assertTrue(body.size() > 64);
            fixture.channel.writeInbound(
                Unpooled.wrappedBuffer(Netty4HttpRequestSizeTestClient.request(body.toByteArray(), "gzip", true, false, 1))
            );
            assertNull(fixture.channel.readInbound());
            assertEquals(1, fixture.writer.pending.size());
            fixture.writer.release();
            fixture.assertRejection();
            assertNull(fixture.channel.readOutbound());
        }
    }

    public void testEncodedRejectionReleasesPartialAggregationBeforeWriteCompletes() {
        try (Fixture fixture = new Fixture()) {
            fixture.channel.writeInbound(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/"));
            ByteBuf partialBody = Unpooled.buffer(8).writeZero(8);
            fixture.channel.writeInbound(new DefaultHttpContent(partialBody));
            assertEquals(1, partialBody.refCnt());
            Netty4HttpObjectAggregator aggregator = fixture.channel.pipeline().get(Netty4HttpObjectAggregator.class);
            aggregator.reject(fixture.channel.pipeline().context(aggregator), true);
            assertEquals("Do not retain the partial request while waiting for the response or draining", 0, partialBody.refCnt());
            assertEquals(1, fixture.writer.pending.size());
            fixture.writer.release();
            fixture.assertRejection();
        }
    }

    public void testEncodedContentLengthAboveLargestLimit() {
        AtomicInteger rejected = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel(
            new Netty4HttpEncodedSizeHandler(Integer.MAX_VALUE, ctx -> rejected.incrementAndGet())
        );
        try {
            DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
            request.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");
            request.headers().set(HttpHeaderNames.CONTENT_LENGTH, (long) Integer.MAX_VALUE + 1);
            assertFalse(channel.writeInbound(request));
            assertEquals("Do not truncate the declared length to an int", 1, rejected.get());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testEncodedSizeCheckPreservesMalformedRequest() {
        EmbeddedChannel channel = new EmbeddedChannel(
            new Netty4HttpEncodedSizeHandler(16, ctx -> fail("Expected malformed request handling"))
        );
        try {
            DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
            request.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");
            request.headers().set(HttpHeaderNames.CONTENT_LENGTH, "invalid");
            request.setDecoderResult(DecoderResult.failure(new IllegalArgumentException("invalid content length")));
            assertTrue(channel.writeInbound(request));
            assertSame(request, channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testRecoverableRejectionsWaitForEveryWriteThenRemoveHandler() {
        try (Fixture fixture = new Fixture()) {
            fixture.receive(OVERSIZED + OVERSIZED);
            Netty4HttpRejectionHandler rejection = fixture.rejection();
            assertNotNull(rejection);
            assertEquals(2, fixture.writer.pending.size());
            fixture.writer.release();
            assertSame(rejection, fixture.rejection());
            fixture.writer.release();
            assertNull(fixture.rejection());
            fixture.assertRejection();
            fixture.assertRejection();
            fixture.receive(GET);
            FullHttpRequest request = fixture.channel.readInbound();
            assertNotNull(request);
            request.release();
            fixture.channel.advanceTimeBy(Netty4HttpRejectionHandler.WRITE_TIMEOUT_SECONDS + 1, TimeUnit.SECONDS);
            fixture.channel.runScheduledPendingTasks();
            assertTrue("A completed rejection must cancel its deadline", fixture.channel.isOpen());
        }
    }

    public void testOversizedExpectationRejectsHeadersAndAllowsNextRequest() {
        try (Fixture fixture = new Fixture()) {
            fixture.receive("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 17\r\nExpect: 100-continue\r\n\r\n");
            assertEquals("Reject without waiting for the body", 1, fixture.writer.pending.size());
            fixture.writer.release();
            fixture.assertRejection();
            fixture.receive(GET);
            FullHttpRequest request = fixture.channel.readInbound();
            assertNotNull(request);
            request.release();
        }
    }

    public void testCloseWaitsForPendingRejection() {
        try (Fixture fixture = new Fixture()) {
            fixture.receive(OVERSIZED);
            ChannelFuture close = fixture.channel.pipeline().close();
            assertFalse(close.isDone());
            assertTrue(fixture.channel.isOpen());
            fixture.writer.release();
            fixture.assertRejection();
            assertTrue(close.isSuccess());
            assertFalse(fixture.channel.isOpen());
        }
    }

    public void testPendingWriteHasDeadlineEvenWhenClientKeepsSending() {
        try (Fixture fixture = new Fixture()) {
            fixture.receive(OVERSIZED);
            fixture.channel.pipeline().close();
            fixture.channel.advanceTimeBy(Netty4HttpRejectionHandler.WRITE_TIMEOUT_SECONDS - 1, TimeUnit.SECONDS);
            fixture.channel.runScheduledPendingTasks();
            assertTrue(fixture.channel.isOpen());
            fixture.receive(GET);
            fixture.channel.advanceTimeBy(2, TimeUnit.SECONDS);
            fixture.channel.runScheduledPendingTasks();
            assertFalse("Input must not extend the pending-write deadline", fixture.channel.isOpen());
            assertNull(fixture.channel.readInbound());
        }
    }

    public void testFailedWriteClosesConnection() {
        try (Fixture fixture = new Fixture()) {
            fixture.receive(OVERSIZED);
            fixture.writer.fail();
            assertFalse(fixture.channel.isOpen());
        }
    }

    public void testTlsShutdownSharesWriteDeadlineAndLateCompletionDoesNotReschedule() {
        try (Fixture fixture = new Fixture()) {
            fixture.receive(OVERSIZED);
            fixture.channel.pipeline().close();
            SslHandler ssl = mock(SslHandler.class);
            ChannelPromise closeNotify = fixture.channel.newPromise();
            when(ssl.closeOutbound()).thenReturn(closeNotify);
            fixture.channel.pipeline().addLast("ssl", ssl);
            fixture.writer.release();
            fixture.assertRejection();
            assertTrue(fixture.channel.isOpen());
            fixture.channel.advanceTimeBy(Netty4HttpRejectionHandler.WRITE_TIMEOUT_SECONDS + 1, TimeUnit.SECONDS);
            fixture.channel.runScheduledPendingTasks();
            assertFalse(fixture.channel.isOpen());
            closeNotify.setSuccess();
            assertEquals(-1, fixture.channel.runScheduledPendingTasks());
        }
    }

    public void testDrainWaitsForIdleSender() {
        try (Fixture fixture = new Fixture(true)) {
            fixture.receive(OVERSIZED);
            fixture.channel.pipeline().close();
            fixture.writer.release();
            fixture.assertRejection();
            assertTrue(((DuplexChannel) fixture.channel).isOutputShutdown());
            fixture.channel.advanceTimeBy(Netty4HttpRejectionHandler.DRAIN_IDLE_SECONDS - 1, TimeUnit.SECONDS);
            fixture.channel.runScheduledPendingTasks();
            assertTrue(fixture.channel.isOpen());
            fixture.receive(GET);
            fixture.channel.advanceTimeBy(2, TimeUnit.SECONDS);
            fixture.channel.runScheduledPendingTasks();
            assertTrue("Recent input must extend the idle drain", fixture.channel.isOpen());
            fixture.channel.advanceTimeBy(Netty4HttpRejectionHandler.DRAIN_IDLE_SECONDS, TimeUnit.SECONDS);
            fixture.channel.runScheduledPendingTasks();
            assertFalse(fixture.channel.isOpen());
            assertNull(fixture.channel.readInbound());
        }
    }

    public void testActiveSenderCannotExtendMaximumDrainDuration() {
        try (Fixture fixture = new Fixture(true)) {
            fixture.receive(OVERSIZED);
            fixture.channel.pipeline().close();
            fixture.writer.release();
            fixture.assertRejection();
            for (int elapsed = 4; elapsed < Netty4HttpRejectionHandler.DRAIN_MAX_SECONDS; elapsed += 4) {
                fixture.channel.advanceTimeBy(4, TimeUnit.SECONDS);
                fixture.receive(GET);
                fixture.channel.runScheduledPendingTasks();
                assertTrue(fixture.channel.isOpen());
            }
            fixture.channel.advanceTimeBy(3, TimeUnit.SECONDS);
            fixture.channel.runScheduledPendingTasks();
            assertFalse("Even continuous input must not extend the maximum drain duration", fixture.channel.isOpen());
            assertNull(fixture.channel.readInbound());
        }
    }

    public void testClientFinCompletesDrainWithoutWaitingForDeadline() {
        try (Fixture fixture = new Fixture(true)) {
            fixture.receive(OVERSIZED);
            fixture.channel.pipeline().close();
            fixture.writer.release();
            fixture.assertRejection();
            assertTrue(fixture.channel.isOpen());
            ((DuplexChannel) fixture.channel).shutdownInput();
            assertFalse(fixture.channel.isOpen());
            assertEquals(-1, fixture.channel.runScheduledPendingTasks());
        }
    }

    public void testTerminalRejectionEnablesDrainingWhenAutoReadWasDisabled() {
        try (Fixture fixture = new Fixture()) {
            fixture.channel.config().setAutoRead(false);
            fixture.receive(OVERSIZED);
            assertTrue(fixture.rejection().isClosing());
            assertTrue(fixture.channel.config().isAutoRead());
            fixture.writer.release();
            fixture.assertRejection();
        }
    }

    public void testTransportShutdownDoesNotWaitForRejection() {
        try (Fixture fixture = new Fixture()) {
            fixture.receive(OVERSIZED);
            when(fixture.transport.lifecycleState()).thenReturn(Lifecycle.State.STOPPED);
            assertTrue(fixture.channel.close().isSuccess());
            assertFalse(fixture.channel.isOpen());
        }
    }

    private static class Fixture implements AutoCloseable {
        final Netty4HttpServerTransport transport = mock(Netty4HttpServerTransport.class);
        final HoldingWriter writer = new HoldingWriter();
        final EmbeddedChannel channel;

        Fixture() {
            this(false);
        }

        Fixture(boolean duplex) {
            this(duplex, 16);
        }

        Fixture(boolean duplex, int maxContentLength) {
            when(transport.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
            ChannelHandler[] handlers = new ChannelHandler[] {
                writer,
                new HttpRequestDecoder(),
                new HttpResponseEncoder(),
                new Netty4HttpObjectAggregator(maxContentLength, transport) };
            channel = duplex ? new DuplexEmbeddedChannel(handlers) : new EmbeddedChannel(handlers);
            channel.freezeTime();
        }

        void receive(String request) {
            channel.writeInbound(Unpooled.copiedBuffer(request, StandardCharsets.US_ASCII));
        }

        Netty4HttpRejectionHandler rejection() {
            return channel.pipeline().get(Netty4HttpRejectionHandler.class);
        }

        void assertRejection() {
            ByteBuf response = channel.readOutbound();
            assertNotNull(response);
            try {
                assertTrue(response.toString(StandardCharsets.US_ASCII).startsWith("HTTP/1.1 413 "));
            } finally {
                response.release();
            }
        }

        @Override
        public void close() {
            when(transport.lifecycleState()).thenReturn(Lifecycle.State.STOPPED);
            channel.finishAndReleaseAll();
        }
    }

    /** A half-closeable channel with EmbeddedChannel's virtual clock, so drain deadlines need no wall-clock waits. */
    private static class DuplexEmbeddedChannel extends EmbeddedChannel implements DuplexChannel {
        private boolean inputShutdown;
        private boolean outputShutdown;

        DuplexEmbeddedChannel(ChannelHandler... handlers) {
            super(handlers);
        }

        @Override
        public boolean isInputShutdown() {
            return inputShutdown;
        }

        @Override
        public boolean isOutputShutdown() {
            return outputShutdown;
        }

        @Override
        public boolean isShutdown() {
            return inputShutdown && outputShutdown;
        }

        @Override
        public ChannelFuture shutdownInput() {
            return shutdownInput(newPromise());
        }

        @Override
        public ChannelFuture shutdownInput(ChannelPromise promise) {
            inputShutdown = true;
            pipeline().fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
            return promise.setSuccess();
        }

        @Override
        public ChannelFuture shutdownOutput() {
            return shutdownOutput(newPromise());
        }

        @Override
        public ChannelFuture shutdownOutput(ChannelPromise promise) {
            outputShutdown = true;
            return promise.setSuccess();
        }

        @Override
        public ChannelFuture shutdown() {
            return shutdown(newPromise());
        }

        @Override
        public ChannelFuture shutdown(ChannelPromise promise) {
            shutdownOutput();
            return shutdownInput(promise);
        }
    }

    private static class HoldingWriter extends ChannelOutboundHandlerAdapter {
        private record Write(Object message, ChannelPromise promise) {
        }

        final Queue<Write> pending = new ArrayDeque<>();
        private ChannelHandlerContext context;

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            context = ctx;
            pending.add(new Write(msg, promise));
        }

        void release() {
            Write write = pending.remove();
            context.writeAndFlush(write.message(), write.promise());
        }

        void fail() {
            Write write = pending.remove();
            ReferenceCountUtil.release(write.message());
            write.promise().tryFailure(new ClosedChannelException());
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            while (pending.isEmpty() == false) {
                fail();
            }
        }
    }
}
