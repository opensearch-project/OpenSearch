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
    private ChannelHandlerContext context;
    private boolean previousAllowHalfClosure;
    private int pendingResponses;
    private boolean terminal;
    private boolean draining;
    private long lastReadNanos;
    private long drainDeadlineNanos;
    private ScheduledFuture<?> timer;

    private Netty4HttpRejectionHandler(BooleanSupplier transportRunning, Timeouts timeouts) {
        this.transportRunning = transportRunning;
        this.timeouts = timeouts;
    }

    /** Returns the handler installed on the pipeline, installing it first if needed. Must be called on the event loop. */
    static Netty4HttpRejectionHandler install(ChannelPipeline pipeline, BooleanSupplier transportRunning, Timeouts timeouts) {
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
        context = ctx;
        if (ctx.channel().config() instanceof SocketChannelConfig config) {
            previousAllowHalfClosure = config.isAllowHalfClosure();
            config.setAllowHalfClosure(true);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        cancelTimer();
    }

    /** Registers a rejection response that is about to be written. */
    void responsePending(boolean closeConnection) {
        pendingResponses++;
        // each response gets the full write bound, also when it follows an earlier one on the same connection
        cancelTimer();
        timer = context.executor().schedule(this::closeNow, timeouts.writeNanos(), TimeUnit.NANOSECONDS);
        if (closeConnection) {
            becomeTerminal();
        }
    }

    /** {@code true} once the connection will be closed: nothing the client sends must be processed anymore. */
    boolean isTerminal() {
        return terminal;
    }

    private void becomeTerminal() {
        if (terminal == false) {
            terminal = true;
            // keep reading: data left unread in the receive buffer would turn the final close into a reset
            context.channel().config().setAutoRead(true);
        }
    }

    /** Listener for the write of a rejection response registered with {@link #responsePending}. */
    void responseWritten(Future<?> future) {
        if (future.isSuccess() == false) {
            closeNow();
            return;
        }
        if (--pendingResponses > 0 || context.channel().isActive() == false) {
            return;
        }
        if (terminal == false) {
            // recoverable: the connection goes back to normal, including Netty closing it on input EOF
            if (context.channel().config() instanceof SocketChannelConfig config) {
                config.setAllowHalfClosure(previousAllowHalfClosure);
            }
            context.pipeline().remove(this);
            return;
        }
        final SslHandler sslHandler = context.pipeline().get(SslHandler.class);
        if (sslHandler == null) {
            shutdownOutput();
        } else {
            // close_notify after the response, so a TLS client sees a complete stream before the FIN
            sslHandler.closeOutbound().addListener(f -> {
                if (f.isSuccess()) {
                    shutdownOutput();
                } else {
                    closeNow();
                }
            });
        }
    }

    private void shutdownOutput() {
        if (context.channel().isActive() == false) {
            return;
        }
        final DuplexChannel channel = (DuplexChannel) context.channel();
        if (channel.isInputShutdown()) {
            // the client already finished sending, nothing is left to drain
            closeNow();
            return;
        }
        channel.shutdownOutput().addListener(f -> {
            if (f.isSuccess() && channel.isActive()) {
                startDraining();
            } else {
                closeNow();
            }
        });
    }

    private void startDraining() {
        draining = true;
        lastReadNanos = System.nanoTime();
        drainDeadlineNanos = lastReadNanos + timeouts.drainMaxNanos();
        cancelTimer();
        checkDrain();
    }

    /** One timer covers both drain bounds; reads only record a timestamp so discarding a buffer stays cheap. */
    private void checkDrain() {
        final long now = System.nanoTime();
        final long remaining = Math.min(drainDeadlineNanos - now, lastReadNanos + timeouts.drainIdleNanos() - now);
        if (remaining <= 0) {
            closeNow();
        } else {
            timer = context.executor().schedule(this::checkDrain, remaining, TimeUnit.NANOSECONDS);
        }
    }

    private void closeNow() {
        cancelTimer();
        terminal = true;
        // starts in front of this handler: bypasses both the deferral below and SslHandler's own close handling
        context.close();
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
        cancelTimer();
        ctx.fireChannelInactive();
    }
}
