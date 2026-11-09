/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
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
 * Makes sure a {@code 413} written before closing an oversized HTTP/1 request's connection reaches the client.
 * <p>
 * The handler is only added to a connection while a rejection is in progress, and it is added in front of every other
 * handler (including TLS), so the regular request path pays nothing for it. While it is installed:
 * <ul>
 *     <li>{@code close()} requests from the rest of the pipeline (a decoder failure on the remainder of the rejected body,
 *     a read timeout, the request handler) are held back until the response is out, so they cannot discard it;</li>
 *     <li>input half-closure is enabled, so a client FIN no longer makes Netty close the connection on its own.</li>
 * </ul>
 * The connection closes the way RFC 9112 section 9.6 describes: everything the client still sends is discarded before
 * it is decrypted or decoded; once the response is written the handler sends TLS
 * {@code close_notify} (if any) and a TCP FIN, then keeps reading until the client closes its side, the client stops
 * sending for {@link Timeouts#drainIdleNanos}, or {@link Timeouts#drainMaxNanos} have elapsed. Closing a socket that
 * still has unread input makes the kernel send a TCP RST, and a RST makes the client's kernel drop a response it has not
 * read yet, which is why the connection is drained rather than closed.
 * <p>
 * The write of the response itself, TLS shutdown and FIN share a deadline of {@link Timeouts#writeNanos}, so a client that
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
    private ChannelHandlerContext context;
    private boolean draining;
    private boolean closed;
    private boolean outputShutdownInitiated;
    private ChannelFuture responseFuture;
    private ChannelFuture shutdownFuture;
    private long lastReadNanos;
    private long drainDeadlineNanos;
    private ScheduledFuture<?> timer;

    private Netty4HttpRejectionHandler(BooleanSupplier transportRunning, Timeouts timeouts) {
        this.transportRunning = transportRunning;
        this.timeouts = timeouts;
    }

    /** Installs the handler for the connection's terminal rejection. Must be called on the event loop. */
    static Netty4HttpRejectionHandler install(ChannelPipeline pipeline, BooleanSupplier transportRunning, Timeouts timeouts) {
        assert pipeline.channel().eventLoop().inEventLoop();
        final Netty4HttpRejectionHandler handler = new Netty4HttpRejectionHandler(transportRunning, timeouts);
        pipeline.addFirst(NAME, handler);
        return handler;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        context = ctx;
        if (ctx.channel().config() instanceof SocketChannelConfig config) {
            config.setAllowHalfClosure(true);
        }
        // Keep reading: data left unread in the receive buffer would turn the final close into a reset.
        ctx.channel().config().setAutoRead(true);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        closed = true;
        cancelTimeouts();
    }

    /** Registers a rejection response and its promise before the response is written. */
    void responsePending(ChannelPromise promise) {
        assert context.executor().inEventLoop();
        assert closed == false && responseFuture == null && timer == null;
        responseFuture = promise;
        timer = context.executor().schedule(this::writeTimedOut, timeouts.writeNanos(), TimeUnit.NANOSECONDS);
        promise.addListener(this::responseWritten);
    }

    private void writeTimedOut() {
        assert context.executor().inEventLoop();
        // Netty may defer promise listeners to avoid excessive listener recursion. Process completed
        // operations before deciding whether the write, TLS shutdown or FIN has timed out.
        if (responseFuture != null && responseFuture.isDone()) {
            responseWritten(responseFuture);
        }
        while (shutdownFuture != null && shutdownFuture.isDone()) {
            shutdownCompleted(shutdownFuture);
        }
        if (draining == false) {
            closeNow();
        }
    }

    /** Listener for the write of a rejection response registered with {@link #responsePending}. */
    private void responseWritten(Future<?> future) {
        assert context.executor().inEventLoop();
        if (closed || future != responseFuture) {
            return;
        }
        responseFuture = null;
        if (future.isSuccess() == false || context.channel().isActive() == false) {
            closeNow();
            return;
        }
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
        lastReadNanos = context.executor().ticker().nanoTime();
        drainDeadlineNanos = lastReadNanos + timeouts.drainMaxNanos();
        cancelTimer();
        checkDrain();
    }

    /** One timer covers both drain bounds; reads only record a timestamp so discarding a buffer stays cheap. */
    private void checkDrain() {
        assert context.executor().inEventLoop();
        final long now = context.executor().ticker().nanoTime();
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
        cancelTimeouts();
        // starts in front of this handler: bypasses both the deferral below and SslHandler's own close handling
        context.close();
    }

    private void cancelTimeouts() {
        cancelTimer();
        responseFuture = null;
        shutdownFuture = null;
    }

    private void cancelTimer() {
        if (timer != null) {
            FutureUtils.cancel(timer);
            timer = null;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ReferenceCountUtil.release(msg);
        if (draining) {
            lastReadNanos = ctx.executor().ticker().nanoTime();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ChannelInputShutdownEvent) {
            // without this handler Netty would have closed the connection: it can only end once the response is out
            if (draining) {
                closeNow();
            }
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Only the socket itself reports here (this handler is first); a peer may reset once it read the response.
        closeNow();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        if (transportRunning.getAsBoolean()) {
            // must not overtake the response; the timer still bounds how long the connection stays open
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
