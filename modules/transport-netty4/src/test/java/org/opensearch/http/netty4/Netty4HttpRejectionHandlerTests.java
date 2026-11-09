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
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Netty4HttpRejectionHandlerTests extends OpenSearchTestCase {

    private enum Stage {
        WRITE,
        TLS,
        FIN
    }

    private EmbeddedDuplexChannel channel;
    private ChannelPromise response;
    private ChannelPromise tlsShutdown;
    private List<Runnable> notifications;
    private boolean transportRunning;

    public void testWriteClosesAtDeadline() {
        reject(null);
        assertClosesAfter(SECONDS.toNanos(30), "An unfinished response must be bounded by its write deadline");
    }

    public void testTlsShutdownKeepsTheResponsesDeadline() {
        reject(Stage.TLS);
        advanceTime(SECONDS.toNanos(20));
        response.setSuccess();
        verify(channel.pipeline().get(SslHandler.class)).closeOutbound();
        assertFalse(tlsShutdown.isDone());
        assertClosesAfter(SECONDS.toNanos(10), "Completing the response must not extend the TLS shutdown deadline");
    }

    public void testCompletedOperationsDoNotTimeOutBeforeListenersRun() {
        for (Stage stage : Stage.values()) {
            reject(stage);
            response.setSuccess();
            if (stage == Stage.TLS) {
                tlsShutdown.setSuccess();
            }
            assertEquals(stage.toString(), 1, notifications.size());
            advanceTime(SECONDS.toNanos(30));
            assertTrue(stage + " completion must proceed to draining", channel.isOpen());
            assertEquals(stage.toString(), 1, channel.outputShutdowns);
            advanceTime(SECONDS.toNanos(1));
            notifications.forEach(Runnable::run);
            assertEquals(stage + " must not shut down output twice", 1, channel.outputShutdowns);
            assertClosesAfter(SECONDS.toNanos(4), stage + " late notification must not restart draining");
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

    public void testInputShutdownWaitsOnlyForResponse() {
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

    /** Starts a fresh rejection, optionally withholding one operation's completion notification. */
    private void reject(Stage deferredStage) {
        closeChannel();
        notifications = new ArrayList<>();
        final EventExecutor listenerExecutor = mock(EventExecutor.class);
        doAnswer(invocation -> {
            notifications.add(invocation.getArgument(0));
            return null;
        }).when(listenerExecutor).execute(any(Runnable.class));
        channel = new EmbeddedDuplexChannel(deferredStage == Stage.FIN ? listenerExecutor : null);
        channel.freezeTime();
        transportRunning = true;
        response = deferredStage == Stage.WRITE ? new DefaultChannelPromise(channel, listenerExecutor) : channel.newPromise();
        tlsShutdown = null;
        if (deferredStage == Stage.TLS) {
            final SslHandler ssl = mock(SslHandler.class);
            tlsShutdown = new DefaultChannelPromise(channel, listenerExecutor);
            when(ssl.closeOutbound()).thenReturn(tlsShutdown);
            channel.pipeline().addLast(ssl);
        }
        // Capture the aggregator's write so completion and notification can be controlled independently.
        final ChannelHandlerContext aggregator = mock(ChannelHandlerContext.class);
        when(aggregator.pipeline()).thenReturn(channel.pipeline());
        when(aggregator.newPromise()).thenReturn(response);
        doAnswer(invocation -> {
            ReferenceCountUtil.release(invocation.getArgument(0));
            return response;
        }).when(aggregator).writeAndFlush(any(), same(response));
        Netty4HttpRejectionHandler.reject(aggregator, () -> transportRunning);
    }

    @After
    public void closeChannel() {
        if (channel != null) {
            channel.unsafe().close(channel.newPromise());
            channel.finishAndReleaseAll();
            response.tryFailure(new IOException("test complete"));
            if (tlsShutdown != null) {
                tlsShutdown.tryFailure(new IOException("test complete"));
            }
            channel = null;
        }
    }

    private static final class EmbeddedDuplexChannel extends EmbeddedChannel implements DuplexChannel {
        private final EventExecutor outputListenerExecutor;
        private int outputShutdowns;
        private boolean inputShutdown;

        private EmbeddedDuplexChannel(EventExecutor outputListenerExecutor) {
            this.outputListenerExecutor = outputListenerExecutor;
        }

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
            return shutdownOutput(outputListenerExecutor == null ? newPromise() : new DefaultChannelPromise(this, outputListenerExecutor));
        }

        @Override
        public ChannelFuture shutdownOutput(ChannelPromise promise) {
            outputShutdowns++;
            return promise.setSuccess();
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
