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
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
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

    public void testLaterRejectionDoesNotExtendPendingWriteTimeout() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise first = pendingResponse(channel, handler);
        channel.advanceTimeBy(20, TimeUnit.SECONDS);
        final ChannelPromise second = pendingResponse(channel, handler);
        try {
            channel.advanceTimeBy(10, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertFalse("A later rejection must not postpone the first write's deadline", channel.isOpen());
        } finally {
            channel.finishAndReleaseAll();
            first.tryFailure(new IOException("test complete"));
            second.tryFailure(new IOException("test complete"));
        }
    }

    public void testEachPendingWriteKeepsItsOwnTimeout() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise first = pendingResponse(channel, handler);
        channel.advanceTimeBy(20, TimeUnit.SECONDS);
        final ChannelPromise second = pendingResponse(channel, handler);
        try {
            channel.advanceTimeBy(5, TimeUnit.SECONDS);
            first.setSuccess();
            channel.advanceTimeBy(5, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue("Completing the first write must cancel only its timeout", channel.isOpen());
            channel.advanceTimeBy(20, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertFalse("The second write must still time out relative to its submission", channel.isOpen());
        } finally {
            channel.finishAndReleaseAll();
            first.tryFailure(new IOException("test complete"));
            second.tryFailure(new IOException("test complete"));
        }
    }

    public void testCompletedWriteDoesNotTimeOutBeforeListenerRuns() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        final List<Runnable> notifications = new ArrayList<>();
        final EventExecutor listenerExecutor = deferListeners(notifications);
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = new DefaultChannelPromise(channel, listenerExecutor);
        handler.responsePending(false, response);
        try {
            response.setSuccess();
            assertEquals("The write is complete but its listener has not run", 1, notifications.size());
            channel.advanceTimeBy(30, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue("A completed recoverable response must not time out", channel.isOpen());
            assertNull(channel.pipeline().context(handler));
            notifications.forEach(Runnable::run);
            assertTrue(channel.isOpen());
        } finally {
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
        handler.responsePending(true, response);
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
        handler.responsePending(true, response);
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
        handler.responsePending(true, response);
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

    public void testFailedWritesRequestCloseOnlyOnce() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final AtomicInteger closeRequests = new AtomicInteger();
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
                closeRequests.incrementAndGet();
                ctx.channel().closeFuture().addListener(f -> promise.trySuccess());
            }
        });
        final ChannelPromise first = pendingResponse(channel, handler);
        final ChannelPromise second = pendingResponse(channel, handler);
        try {
            first.setFailure(new IOException("first write failed"));
            second.setFailure(new IOException("second write failed"));
            channel.advanceTimeBy(30, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertEquals("Failed writes and cancelled timers must not restart closure", 1, closeRequests.get());
        } finally {
            channel.unsafe().close(channel.newPromise());
            channel.finishAndReleaseAll();
        }
    }

    public void testRemovedHandlerIgnoresLateWriteCompletion() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        final Netty4HttpRejectionHandler handler = install(channel);
        final ChannelPromise response = pendingResponse(channel, handler);
        try {
            channel.pipeline().remove(handler);
            response.setFailure(new IOException("write completed after removal"));
            channel.advanceTimeBy(30, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue("A removed handler must not close the channel from a stale listener or timer", channel.isOpen());
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
        handler.responsePending(false, promise);
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
