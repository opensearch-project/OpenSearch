/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.EventExecutor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Netty4HttpRejectionHandlerTests extends OpenSearchTestCase {

    public void testWriteClosesAtDeadline() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = pendingResponse(channel, handler);
        try {
            channel.advanceTimeBy(TimeUnit.SECONDS.toNanos(30) - 1, TimeUnit.NANOSECONDS);
            channel.runScheduledPendingTasks();
            assertTrue("The response has the entire write interval to complete", channel.isOpen());
            channel.advanceTimeBy(1, TimeUnit.NANOSECONDS);
            channel.runScheduledPendingTasks();
            assertFalse("An unfinished response must not retain the connection past its deadline", channel.isOpen());
        } finally {
            channel.finishAndReleaseAll();
            response.tryFailure(new IOException("test complete"));
        }
    }

    public void testCompletedWriteDoesNotTimeOutBeforeListenerRuns() {
        final EmbeddedDuplexChannel channel = new EmbeddedDuplexChannel(null);
        channel.freezeTime();
        final List<Runnable> notifications = new ArrayList<>();
        final EventExecutor listenerExecutor = deferListeners(notifications);
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = new DefaultChannelPromise(channel, listenerExecutor);
        handler.responsePending(response);
        try {
            response.setSuccess();
            assertEquals("The write is complete but its listener has not run", 1, notifications.size());
            channel.advanceTimeBy(30, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue("A completed response must proceed to FIN and draining", channel.isOpen());
            assertEquals(1, channel.outputShutdowns);
            notifications.forEach(Runnable::run);
            assertEquals("A delayed write listener must not shut down output twice", 1, channel.outputShutdowns);
            assertTrue(channel.isOpen());
        } finally {
            channel.unsafe().close(channel.newPromise());
            channel.finishAndReleaseAll();
        }
    }

    public void testTlsShutdownKeepsTheCompletedWritesDeadline() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        final SslHandler ssl = mock(SslHandler.class);
        final ChannelPromise shutdown = channel.newPromise();
        when(ssl.closeOutbound()).thenReturn(shutdown);
        channel.pipeline().addLast(ssl);
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = channel.newPromise();
        handler.responsePending(response);
        try {
            channel.advanceTimeBy(20, TimeUnit.SECONDS);
            response.setSuccess();
            verify(ssl).closeOutbound();
            assertFalse(shutdown.isDone());
            assertTrue(channel.isOpen());
            channel.advanceTimeBy(10, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertFalse("A completed response must not leave TLS shutdown without a timeout", channel.isOpen());
        } finally {
            channel.finishAndReleaseAll();
            shutdown.tryFailure(new IOException("test complete"));
        }
    }

    public void testCompletedTlsShutdownDoesNotTimeOutBeforeListenerRuns() {
        final EmbeddedDuplexChannel channel = new EmbeddedDuplexChannel(null);
        channel.freezeTime();
        final List<Runnable> notifications = new ArrayList<>();
        final SslHandler ssl = mock(SslHandler.class);
        final ChannelPromise shutdown = new DefaultChannelPromise(channel, deferListeners(notifications));
        when(ssl.closeOutbound()).thenReturn(shutdown);
        channel.pipeline().addLast(ssl);
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = channel.newPromise();
        handler.responsePending(response);
        try {
            response.setSuccess();
            shutdown.setSuccess();
            assertEquals(1, notifications.size());
            channel.advanceTimeBy(30, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue("Completed TLS shutdown must proceed to FIN and draining", channel.isOpen());
            assertEquals(1, channel.outputShutdowns);
            notifications.forEach(Runnable::run);
            assertEquals("A delayed TLS listener must not shut down output twice", 1, channel.outputShutdowns);
            assertTrue(channel.isOpen());
        } finally {
            channel.unsafe().close(channel.newPromise());
            channel.finishAndReleaseAll();
        }
    }

    public void testCompletedFinDoesNotTimeOutBeforeListenerRuns() {
        final List<Runnable> notifications = new ArrayList<>();
        final EmbeddedDuplexChannel channel = new EmbeddedDuplexChannel(deferListeners(notifications));
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = channel.newPromise();
        handler.responsePending(response);
        try {
            response.setSuccess();
            assertEquals(1, notifications.size());
            channel.advanceTimeBy(30, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue("A completed FIN must proceed to draining", channel.isOpen());
            notifications.forEach(Runnable::run);
            assertTrue(channel.isOpen());
            assertEquals(1, channel.outputShutdowns);
        } finally {
            channel.unsafe().close(channel.newPromise());
            channel.finishAndReleaseAll();
        }
    }

    public void testDrainClosesAtIdleDeadline() {
        final EmbeddedDuplexChannel channel = new EmbeddedDuplexChannel(null);
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = channel.newPromise();
        handler.responsePending(response);
        try {
            response.setSuccess();
            assertTrue(channel.isOutputShutdown());
            channel.advanceTimeBy(TimeUnit.SECONDS.toNanos(5) - 1, TimeUnit.NANOSECONDS);
            channel.runScheduledPendingTasks();
            assertTrue("The entire idle interval must be available for draining", channel.isOpen());
            channel.advanceTimeBy(1, TimeUnit.NANOSECONDS);
            channel.runScheduledPendingTasks();
            assertFalse("An idle peer must not retain the connection after the drain deadline", channel.isOpen());
        } finally {
            channel.unsafe().close(channel.newPromise());
            channel.finishAndReleaseAll();
        }
    }

    public void testDrainReadRestartsIdleInterval() {
        final EmbeddedDuplexChannel channel = new EmbeddedDuplexChannel(null);
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = channel.newPromise();
        handler.responsePending(response);
        try {
            response.setSuccess();
            channel.advanceTimeBy(4, TimeUnit.SECONDS);
            final ByteBuf discarded = Unpooled.wrappedBuffer(new byte[] { 1 });
            assertFalse(channel.writeInbound(discarded));
            assertEquals(0, discarded.refCnt());
            channel.advanceTimeBy(1, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue("The original idle deadline must observe the more recent read", channel.isOpen());
            channel.advanceTimeBy(TimeUnit.SECONDS.toNanos(4) - 1, TimeUnit.NANOSECONDS);
            channel.runScheduledPendingTasks();
            assertTrue(channel.isOpen());
            channel.advanceTimeBy(1, TimeUnit.NANOSECONDS);
            channel.runScheduledPendingTasks();
            assertFalse("The idle interval must be measured from the last read", channel.isOpen());
        } finally {
            channel.unsafe().close(channel.newPromise());
            channel.finishAndReleaseAll();
        }
    }

    public void testDrainMaximumDeadlineIsNotExtendedByReads() {
        final EmbeddedDuplexChannel channel = new EmbeddedDuplexChannel(null);
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = channel.newPromise();
        handler.responsePending(response);
        try {
            response.setSuccess();
            for (int elapsedSeconds = 4; elapsedSeconds < 30; elapsedSeconds += 4) {
                channel.advanceTimeBy(4, TimeUnit.SECONDS);
                channel.runScheduledPendingTasks();
                assertTrue("Regular reads must keep the connection alive until the maximum deadline", channel.isOpen());
                final ByteBuf discarded = Unpooled.wrappedBuffer(new byte[] { 1 });
                assertFalse(channel.writeInbound(discarded));
                assertEquals(0, discarded.refCnt());
            }
            channel.advanceTimeBy(TimeUnit.SECONDS.toNanos(2) - 1, TimeUnit.NANOSECONDS);
            channel.runScheduledPendingTasks();
            assertTrue(channel.isOpen());
            channel.advanceTimeBy(1, TimeUnit.NANOSECONDS);
            channel.runScheduledPendingTasks();
            assertFalse("A continuously sending peer must still be bounded by the maximum drain interval", channel.isOpen());
        } finally {
            channel.unsafe().close(channel.newPromise());
            channel.finishAndReleaseAll();
        }
    }

    public void testFailedWriteClosesConnection() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = pendingResponse(channel, handler);
        try {
            response.setFailure(new IOException("write failed"));
            assertFalse("A failed response must close without waiting for its deadline", channel.isOpen());
            channel.advanceTimeBy(30, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static Netty4HttpRejectionHandler install(EmbeddedChannel channel) {
        return Netty4HttpRejectionHandler.install(channel.pipeline(), () -> true, Netty4HttpRejectionHandler.Timeouts.DEFAULT);
    }

    private static EventExecutor deferListeners(List<Runnable> notifications) {
        final EventExecutor executor = mock(EventExecutor.class);
        doAnswer(invocation -> {
            notifications.add(invocation.getArgument(0));
            return null;
        }).when(executor).execute(any(Runnable.class));
        return executor;
    }

    private static ChannelPromise pendingResponse(EmbeddedChannel channel, Netty4HttpRejectionHandler handler) {
        final ChannelPromise promise = channel.newPromise();
        handler.responsePending(promise);
        return promise;
    }

    private static final class EmbeddedDuplexChannel extends EmbeddedChannel implements DuplexChannel {
        private final EventExecutor outputListenerExecutor;
        private int outputShutdowns;

        private EmbeddedDuplexChannel(EventExecutor outputListenerExecutor) {
            this.outputListenerExecutor = outputListenerExecutor;
        }

        @Override
        public boolean isInputShutdown() {
            return false;
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
