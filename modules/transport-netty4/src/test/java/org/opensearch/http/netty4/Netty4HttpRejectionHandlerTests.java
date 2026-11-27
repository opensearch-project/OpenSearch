/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslHandler;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Netty4HttpRejectionHandlerTests extends OpenSearchTestCase {

    private enum Stage {
        TLS,
        FIN
    }

    private EmbeddedDuplexChannel channel;
    private ChannelPromise response;
    private ChannelPromise shutdown;
    private boolean transportRunning;

    public void testWriteClosesAtDeadline() {
        reject(null);
        assertClosesAfter(SECONDS.toNanos(30), "An unfinished response must be bounded by its write deadline");
    }

    public void testShutdownKeepsTheResponsesDeadline() {
        for (Stage stage : Stage.values()) {
            reject(stage);
            advanceTime(SECONDS.toNanos(20));
            response.setSuccess();
            assertFalse(shutdown.isDone());
            assertClosesAfter(SECONDS.toNanos(10), "Completing the response must not extend the " + stage + " shutdown deadline");
        }
    }

    public void testDrainDeadlines() {
        assertDrainDeadline(5, 0);  // idle from the start
        assertDrainDeadline(9, 1);  // one read at 4s renews the idle interval
        assertDrainDeadline(30, 7); // reads every 4s cannot extend the maximum interval
    }

    private void assertDrainDeadline(int deadlineSeconds, int reads) {
        reject(null);
        response.setSuccess();
        assertTrue(channel.isOutputShutdown());
        for (int i = 0; i < reads; i++) {
            advanceTime(SECONDS.toNanos(4));
            assertTrue("Regular input must keep the drain alive", channel.isOpen());
            final ByteBuf discarded = Unpooled.wrappedBuffer(new byte[] { 1 });
            assertFalse(channel.writeInbound(discarded));
            assertEquals(0, discarded.refCnt());
        }
        int elapsed = reads * 4;
        if (reads > 0) {
            advanceTime(SECONDS.toNanos(1));
            elapsed++;
            assertTrue("The earlier idle deadline must observe the last read", channel.isOpen());
        }
        assertClosesAfter(SECONDS.toNanos(deadlineSeconds - elapsed), "Drain deadline with " + reads + " reads");
    }

    public void testCloseWaitsForResponseAndDrain() {
        reject(null);
        // EmbeddedChannel.close() cancels scheduled tasks itself; exercise the pipeline's close operation directly.
        final ChannelFuture close = channel.pipeline().close();
        assertFalse(close.isDone());
        response.setSuccess();
        assertFalse(close.isDone());
        assertClosesAfter(SECONDS.toNanos(5), "A deferred close must wait for the response and bounded drain");
        assertTrue(close.isSuccess());
    }

    public void testInputShutdownWaitsForPendingWrites() {
        for (boolean responseWritten : new boolean[] { false, true }) {
            reject(null);
            if (responseWritten) {
                response.setSuccess();
            }
            channel.inputShutdown = true;
            channel.pipeline().fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
            assertEquals("Input EOF must not overtake the response", responseWritten == false, channel.isOpen());
            response.trySuccess();
            assertFalse(channel.isOpen());
        }
        for (Stage stage : new Stage[] { Stage.TLS, Stage.FIN }) {
            reject(stage);
            response.setSuccess();
            channel.inputShutdown = true;
            channel.pipeline().fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
            assertTrue("Input EOF must wait for pending shutdown work", channel.isOpen());
            shutdown.setSuccess();
            assertFalse(stage + ": completed shutdown must not wait to drain input that already reached EOF", channel.isOpen());
        }
    }

    public void testTransportStopBypassesCloseDeferral() {
        reject(null);
        transportRunning = false;
        assertTrue(channel.pipeline().close().isSuccess());
        assertFalse(channel.isOpen());
    }

    public void testSocketFailureClosesImmediately() {
        reject(null);
        channel.pipeline().fireExceptionCaught(new IOException("socket failure"));
        assertFalse(channel.isOpen());
    }

    public void testFailedWriteClosesImmediately() {
        reject(null);
        response.setFailure(new IOException("write failed"));
        assertFalse(channel.isOpen());
    }

    public void testHeaderRejectionResumesReads() {
        rejectRequest();
        assertNotNull(channel.pipeline().get(Netty4HttpRejectionHandler.class));
        assertTrue("Draining must resume reads", channel.config().isAutoRead());
        assertTrue("The rejected request body must still be draining", channel.isOpen());
        assertTrue(channel.isOutputShutdown());
    }

    public void testHttpGuardDiscardsAlreadyDecodedRequests() {
        channel = new EmbeddedDuplexChannel();
        channel.freezeTime();
        final Netty4HttpRejectionHandler rejection = new Netty4HttpRejectionHandler(() -> true);
        channel.pipeline().addLast(rejection.newHttpHandler());
        rejection.reject(channel, channel.newPromise().setSuccess());
        final DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.POST,
            "/",
            Unpooled.buffer(1).writeByte(1)
        );
        // Bypass raw-input draining to exercise the sole HTTP guard, as on the TLS/default pipeline.
        channel.pipeline().context(Netty4HttpRejectionHandler.NAME).fireChannelRead(request);
        assertTrue(channel.inboundMessages().isEmpty());
        assertEquals(0, request.refCnt());
    }

    public void testInactiveChannelCancelsDeadlineAndIgnoresLateCompletion() {
        for (Stage stage : new Stage[] { null, Stage.TLS, Stage.FIN }) {
            reject(stage);
            if (stage != null) {
                response.setSuccess();
            }
            final int shutdowns = channel.outputShutdowns;
            channel.unsafe().close(channel.newPromise());
            assertEquals(-1, channel.runScheduledPendingTasks());
            (stage == null ? response : shutdown).setSuccess();
            assertEquals(shutdowns, channel.outputShutdowns);
            assertEquals(-1, channel.runScheduledPendingTasks());
        }
    }

    public void testFatalErrorsAreRethrownAfterRejection() throws Exception {
        assertFatalErrorRethrown(rejectRequest());
        reject(null);
        assertFatalErrorRethrown(channel.pipeline().context(Netty4HttpRejectionHandler.class));
    }

    private void assertFatalErrorRethrown(ChannelHandlerContext ctx) throws Exception {
        final Error error = new OutOfMemoryError("simulated fatal error");
        final CompletableFuture<Throwable> uncaught = new CompletableFuture<>();
        final ThreadGroup group = new ThreadGroup(getTestName()) {
            @Override
            public void uncaughtException(Thread thread, Throwable cause) {
                uncaught.complete(cause);
            }
        };
        ctx.pipeline().addBefore(ctx.name(), "exception_source", new ChannelInboundHandlerAdapter());
        final ChannelHandlerContext source = ctx.pipeline().context("exception_source");
        // The fatal-error helper's child thread inherits this group, leaving the test runner's handler untouched.
        final Thread thread = new Thread(group, () -> source.fireExceptionCaught(new DecoderException(error)));
        thread.start();
        try {
            assertSame(error, uncaught.get(10, SECONDS));
        } finally {
            thread.join(10000);
        }
        assertFalse(thread.isAlive());
    }

    /** Netty chooses a terminal rejection because auto-read is off; observing its response must resume reads. */
    private ChannelHandlerContext rejectRequest() {
        closeChannel();
        channel = new EmbeddedDuplexChannel();
        channel.freezeTime();
        channel.config().setAutoRead(false);
        final Netty4HttpRejectionHandler rejection = new Netty4HttpRejectionHandler(() -> true);
        channel.pipeline().addLast("upgrade_rejection", rejection.newHttpHandler());
        channel.pipeline()
            .addLast("aggregator", Netty4HttpObjectAggregator.newUpgradeHandler(new HttpServerCodec(), protocol -> null, 1, rejection));
        channel.pipeline().addLast("http_rejection_guard", rejection.newHttpHandler());
        final DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
        HttpUtil.setContentLength(request, 2);
        request.headers().set(HttpHeaderNames.UPGRADE, "h2c");
        assertFalse(channel.writeInbound(request));
        return channel.pipeline().context("aggregator");
    }

    private void advanceTime(long nanos) {
        channel.advanceTimeBy(nanos, NANOSECONDS);
        channel.runScheduledPendingTasks();
    }

    private void assertClosesAfter(long nanos, String message) {
        advanceTime(nanos - 1);
        assertTrue(message + " (before deadline)", channel.isOpen());
        advanceTime(1);
        assertFalse(message + " (at deadline)", channel.isOpen());
    }

    /** Starts a fresh rejection, optionally leaving TLS or TCP output shutdown pending. */
    private void reject(Stage pendingShutdown) {
        closeChannel();
        channel = new EmbeddedDuplexChannel();
        channel.freezeTime();
        transportRunning = true;
        response = channel.newPromise();
        shutdown = channel.newPromise();
        if (pendingShutdown == Stage.TLS) {
            final SslHandler ssl = mock(SslHandler.class);
            when(ssl.closeOutbound()).thenReturn(shutdown);
            channel.pipeline().addLast(ssl);
        } else if (pendingShutdown == Stage.FIN) {
            channel.outputShutdown = shutdown;
        }
        new Netty4HttpRejectionHandler(() -> transportRunning).reject(channel, response);
    }

    @After
    public void closeChannel() {
        if (channel != null) {
            channel.unsafe().close(channel.newPromise());
            channel.finishAndReleaseAll();
            if (response != null) {
                response.tryFailure(new IOException("test complete"));
                response = null;
            }
            if (shutdown != null) {
                shutdown.tryFailure(new IOException("test complete"));
                shutdown = null;
            }
            channel = null;
        }
    }

    private static final class EmbeddedDuplexChannel extends EmbeddedChannel implements DuplexChannel {
        private ChannelPromise outputShutdown;
        private int outputShutdowns;
        private boolean inputShutdown;

        @Override
        public boolean isInputShutdown() {
            return inputShutdown;
        }

        @Override
        public boolean isOutputShutdown() {
            return outputShutdowns > 0;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public ChannelFuture shutdownOutput() {
            outputShutdowns++;
            return outputShutdown == null ? newSucceededFuture() : outputShutdown;
        }

        @Override
        public ChannelFuture shutdownOutput(ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture shutdownInput() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture shutdownInput(ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChannelFuture shutdown(ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }
    }
}
