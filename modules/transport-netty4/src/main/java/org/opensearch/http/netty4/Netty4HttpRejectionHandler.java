/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.util.concurrent.FutureUtils;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Makes sure a {@code 413} written for an oversized HTTP/1 request reaches the client.
 * <p>
 * The handler is only added to a connection while a rejection is in progress, and it is added in front of every other
 * handler (including TLS), so the regular request path pays nothing for it. While it is installed:
 * <ul>
 *     <li>{@code close()} requests from the rest of the pipeline (a decoder failure on the remainder of the rejected body,
 *     a read timeout, the request handler) are held back until the response is out, so they cannot discard it;</li>
 *     <li>input half-closure is enabled, so a client FIN no longer makes Netty close the connection on its own.</li>
 * </ul>
 * A <em>recoverable</em> rejection (the connection can be reused, for example because the request declared a too large
 * {@code Content-Length} and asked for {@code 100-continue}) removes the handler again once the response is written.
 * <p>
 * A <em>terminal</em> rejection closes the connection the way RFC 9112 section 9.6 describes: everything the client still
 * sends is discarded before it is decrypted or decoded; once the response is written the handler sends TLS
 * {@code close_notify} (if any) and a TCP FIN, then keeps reading until the client closes its side, the client stops
 * sending for {@link Timeouts#drainIdleNanos}, or {@link Timeouts#drainMaxNanos} have elapsed. Closing a socket that
 * still has unread input makes the kernel send a TCP RST, and a RST makes the client's kernel drop a response it has not
 * read yet, which is why the connection is drained rather than closed.
 * <p>
 * The write of the response itself (and the TLS shutdown) is bounded by {@link Timeouts#writeNanos}, so a client that
 * never reads cannot pin the connection.
 */
final class Netty4HttpRejectionHandler extends ChannelDuplexHandler {

    static final String NAME = "http_rejection";

    /**
     * Bounds of a rejection. The drain defaults match the defaults of NGINX's {@code lingering_timeout} and
     * {@code lingering_time}.
     */
    record Timeouts(long writeNanos, long drainIdleNanos, long drainMaxNanos) {
        static final Timeouts DEFAULT = new Timeouts(
            TimeUnit.SECONDS.toNanos(30),
            TimeUnit.SECONDS.toNanos(5),
            TimeUnit.SECONDS.toNanos(30)
        );
    }

    private final BooleanSupplier transportRunning;
    private final Timeouts timeouts;
    private final Map<Future<?>, ScheduledFuture<?>> pendingResponses = new IdentityHashMap<>();
    private final ChannelFutureListener responseListener = this::responseWritten;
    private ChannelHandlerContext context;
    private boolean previousAllowHalfClosure;
    private boolean terminal;
    private boolean draining;
    private boolean closed;
    private boolean outputShutdownInitiated;
    private ChannelFuture shutdownFuture;
    private long lastReadNanos;
    private long drainDeadlineNanos;
    private ScheduledFuture<?> timer;

    private Netty4HttpRejectionHandler(BooleanSupplier transportRunning, Timeouts timeouts) {
        this.transportRunning = transportRunning;
        this.timeouts = timeouts;
    }

    /** Returns the handler installed on the pipeline, installing it first if needed. Must be called on the event loop. */
    static Netty4HttpRejectionHandler install(ChannelPipeline pipeline, BooleanSupplier transportRunning, Timeouts timeouts) {
        assert pipeline.channel().eventLoop().inEventLoop();
        final Netty4HttpRejectionHandler existing = pipeline.get(Netty4HttpRejectionHandler.class);
        if (existing != null) {
            return existing;
        }
        final Netty4HttpRejectionHandler handler = new Netty4HttpRejectionHandler(transportRunning, timeouts);
        pipeline.addFirst(NAME, handler);
        return handler;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        context = ctx;
        if (ctx.channel().config() instanceof SocketChannelConfig config) {
            previousAllowHalfClosure = config.isAllowHalfClosure();
            config.setAllowHalfClosure(true);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        closed = true;
        cancelTimeouts();
        if (terminal == false && ctx.channel().config() instanceof SocketChannelConfig config) {
            config.setAllowHalfClosure(previousAllowHalfClosure);
        }
    }

    /** Registers a rejection response and its promise before the response is written. */
    void responsePending(boolean closeConnection, ChannelPromise promise) {
        assert context.executor().inEventLoop();
        assert closed == false && draining == false;
        // As in Netty's WriteTimeoutHandler, each pending write keeps its own deadline. New rejections
        // must not postpone an older blocked write, and completing one write must not cancel another's timeout.
        pendingResponses.put(
            promise,
            context.executor().schedule(() -> writeTimedOut(promise), timeouts.writeNanos(), TimeUnit.NANOSECONDS)
        );
        if (closeConnection) {
            becomeTerminal();
        }
        promise.addListener(responseListener);
    }

    private void writeTimedOut(ChannelPromise promise) {
        assert context.executor().inEventLoop();
        if (promise.isDone()) {
            // Netty may defer promise listeners to avoid excessive listener recursion. Process a completed
            // write before deciding whether to close, including starting terminal shutdown when necessary.
            responseWritten(promise);
            while (shutdownFuture != null && shutdownFuture.isDone()) {
                shutdownCompleted(shutdownFuture);
            }
            if (terminal && draining == false && pendingResponses.isEmpty()) {
                closeNow();
            }
        } else {
            closeNow();
        }
    }

    /** {@code true} once the connection will be closed: nothing the client sends must be processed anymore. */
    boolean isTerminal() {
        return terminal;
    }

    private void becomeTerminal() {
        assert context.executor().inEventLoop();
        if (terminal == false) {
            terminal = true;
            // keep reading: data left unread in the receive buffer would turn the final close into a reset
            context.channel().config().setAutoRead(true);
        }
    }

    /** Listener for the write of a rejection response registered with {@link #responsePending}. */
    private void responseWritten(Future<?> future) {
        assert context.executor().inEventLoop();
        final ScheduledFuture<?> writeTimeout = pendingResponses.remove(future);
        if (writeTimeout == null) {
            // Closing or removing the handler already cancelled this write's timeout.
            return;
        }
        if (future.isSuccess() == false) {
            FutureUtils.cancel(writeTimeout);
            closeNow();
            return;
        }
        if (pendingResponses.isEmpty() == false || context.channel().isActive() == false) {
            FutureUtils.cancel(writeTimeout);
            return;
        }
        if (terminal == false) {
            // recoverable: the connection goes back to normal, including Netty closing it on input EOF
            FutureUtils.cancel(writeTimeout);
            context.pipeline().remove(this);
            return;
        }
        // TLS shutdown and the FIN must finish within the final pending response's write deadline.
        timer = writeTimeout;
        final SslHandler sslHandler = context.pipeline().get(SslHandler.class);
        if (sslHandler == null) {
            shutdownOutput();
        } else {
            // close_notify after the response, so a TLS client sees a complete stream before the FIN
            shutdownFuture = sslHandler.closeOutbound();
            shutdownFuture.addListener(this::shutdownCompleted);
        }
    }

    private void shutdownOutput() {
        assert context.executor().inEventLoop();
        if (closed || context.channel().isActive() == false) {
            return;
        }
        final DuplexChannel channel = (DuplexChannel) context.channel();
        if (channel.isInputShutdown()) {
            // the client already finished sending, nothing is left to drain
            closeNow();
            return;
        }
        outputShutdownInitiated = true;
        shutdownFuture = channel.shutdownOutput();
        shutdownFuture.addListener(this::shutdownCompleted);
    }

    private void shutdownCompleted(Future<?> future) {
        assert context.executor().inEventLoop();
        if (closed || future != shutdownFuture) {
            return;
        }
        shutdownFuture = null;
        if (future.isSuccess() == false || context.channel().isActive() == false) {
            closeNow();
        } else if (outputShutdownInitiated) {
            startDraining();
        } else {
            shutdownOutput();
        }
    }

    private void startDraining() {
        assert context.executor().inEventLoop();
        draining = true;
        lastReadNanos = System.nanoTime();
        drainDeadlineNanos = lastReadNanos + timeouts.drainMaxNanos();
        cancelTimer();
        checkDrain();
    }

    /** One timer covers both drain bounds; reads only record a timestamp so discarding a buffer stays cheap. */
    private void checkDrain() {
        assert context.executor().inEventLoop();
        final long now = System.nanoTime();
        final long remaining = Math.min(drainDeadlineNanos - now, lastReadNanos + timeouts.drainIdleNanos() - now);
        if (remaining <= 0) {
            closeNow();
        } else {
            timer = context.executor().schedule(this::checkDrain, remaining, TimeUnit.NANOSECONDS);
        }
    }

    private void closeNow() {
        assert context.executor().inEventLoop();
        if (closed) {
            return;
        }
        closed = true;
        terminal = true;
        cancelTimeouts();
        // starts in front of this handler: bypasses both the deferral below and SslHandler's own close handling
        context.close();
    }

    private void cancelTimeouts() {
        cancelTimer();
        shutdownFuture = null;
        for (ScheduledFuture<?> timeout : pendingResponses.values()) {
            FutureUtils.cancel(timeout);
        }
        pendingResponses.clear();
    }

    private void cancelTimer() {
        if (timer != null) {
            FutureUtils.cancel(timer);
            timer = null;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (terminal) {
            ReferenceCountUtil.release(msg);
            if (draining) {
                lastReadNanos = System.nanoTime();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ChannelInputShutdownEvent) {
            // without this handler Netty would have closed the connection: it can only end once the response is out
            becomeTerminal();
            if (draining) {
                closeNow();
            }
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (terminal) {
            // only the socket itself reports here (this handler is first); a peer may reset once it read the response
            closeNow();
        } else {
            ctx.fireExceptionCaught(cause);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        if (transportRunning.getAsBoolean()) {
            // must not overtake the response; the timer still bounds how long the connection stays open
            becomeTerminal();
            ctx.channel().closeFuture().addListener(f -> promise.trySuccess());
        } else {
            ctx.close(promise);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        closed = true;
        cancelTimeouts();
        ctx.fireChannelInactive();
    }
}
