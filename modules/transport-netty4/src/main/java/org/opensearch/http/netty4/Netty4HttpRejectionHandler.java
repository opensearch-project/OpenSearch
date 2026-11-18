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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Delivers a terminal HTTP/1 {@code 413} before closing the connection.
 * <p>
 * Installed only for a terminal rejection, before TLS and HTTP decoding. It discards remaining input, defers pipeline
 * close requests, and enables input half-closure so a client FIN cannot interrupt the response.
 * <p>
 * Response write, TLS {@code close_notify}, and TCP output shutdown run in order under one {@link #WRITE_TIMEOUT_NANOS}
 * deadline. The connection then drains until peer EOF, {@link #DRAIN_IDLE_NANOS} of inactivity, or {@link #DRAIN_MAX_NANOS}
 * total. Draining avoids a TCP reset from unread input discarding the response, as described in RFC 9112 section 9.6.
 */
final class Netty4HttpRejectionHandler extends ChannelDuplexHandler {

    static final String NAME = "http_rejection";

    private static final long WRITE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    // Match the defaults of NGINX's lingering_timeout and lingering_time.
    private static final long DRAIN_IDLE_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final long DRAIN_MAX_NANOS = TimeUnit.SECONDS.toNanos(30);

    private enum Phase {
        RESPONSE,
        TLS_SHUTDOWN,
        OUTPUT_SHUTDOWN,
        DRAINING,
        CLOSED
    }

    private final BooleanSupplier transportRunning;
    private ChannelHandlerContext context;
    private Phase phase = Phase.RESPONSE;
    private ChannelFuture pendingOperation;
    private long lastReadNanos;
    private long drainDeadlineNanos;
    private ScheduledFuture<?> timer;

    private Netty4HttpRejectionHandler(BooleanSupplier transportRunning) {
        this.transportRunning = transportRunning;
    }

    /** Installs close protection and a deadline before writing the connection's terminal rejection. */
    static void reject(ChannelHandlerContext aggregatorContext, BooleanSupplier transportRunning) {
        final ChannelPipeline pipeline = aggregatorContext.pipeline();
        assert pipeline.channel().eventLoop().inEventLoop();
        final Netty4HttpRejectionHandler handler = new Netty4HttpRejectionHandler(transportRunning);
        pipeline.addFirst(NAME, handler);
        final ChannelPromise promise = aggregatorContext.newPromise();
        handler.timer = handler.context.executor().schedule(handler::writeTimedOut, WRITE_TIMEOUT_NANOS, TimeUnit.NANOSECONDS);
        handler.await(Phase.RESPONSE, promise);
        final FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
            Unpooled.EMPTY_BUFFER
        );
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        aggregatorContext.writeAndFlush(response, promise);
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
        finish();
    }

    private void writeTimedOut() {
        assert context.executor().inEventLoop();
        // Netty may defer promise listeners to avoid excessive listener recursion. Process completed
        // operations before deciding whether the write, TLS shutdown or FIN has timed out.
        while (pendingOperation != null && pendingOperation.isDone()) {
            operationCompleted(pendingOperation);
        }
        if (phase != Phase.DRAINING) {
            closeNow();
        }
    }

    private void await(Phase nextPhase, ChannelFuture future) {
        // Starting an operation can synchronously close the channel.
        if (phase == Phase.CLOSED) {
            return;
        }
        phase = nextPhase;
        pendingOperation = future;
        future.addListener(this::operationCompleted);
    }

    private void operationCompleted(Future<?> future) {
        assert context.executor().inEventLoop();
        if (future != pendingOperation) {
            return; // already processed by the deadline, or the connection closed
        }
        pendingOperation = null;
        if (future.isSuccess() == false || context.channel().isActive() == false) {
            closeNow();
            return;
        }
        switch (phase) {
            case RESPONSE -> {
                final SslHandler ssl = context.pipeline().get(SslHandler.class);
                if (ssl == null) {
                    shutdownOutput();
                } else {
                    await(Phase.TLS_SHUTDOWN, ssl.closeOutbound());
                }
            }
            case TLS_SHUTDOWN -> shutdownOutput();
            case OUTPUT_SHUTDOWN -> {
                phase = Phase.DRAINING;
                lastReadNanos = context.executor().ticker().nanoTime();
                drainDeadlineNanos = lastReadNanos + DRAIN_MAX_NANOS;
                cancelTimer();
                checkDrain();
            }
            default -> throw new IllegalStateException("Unexpected rejection phase: " + phase);
        }
    }

    private void shutdownOutput() {
        final DuplexChannel channel = (DuplexChannel) context.channel();
        if (channel.isInputShutdown()) {
            // the client already finished sending, nothing is left to drain
            closeNow();
        } else {
            await(Phase.OUTPUT_SHUTDOWN, channel.shutdownOutput());
        }
    }

    /** One timer covers both drain bounds; reads only record a timestamp so discarding a buffer stays cheap. */
    private void checkDrain() {
        assert context.executor().inEventLoop();
        final long now = context.executor().ticker().nanoTime();
        final long remaining = Math.min(drainDeadlineNanos - now, lastReadNanos + DRAIN_IDLE_NANOS - now);
        if (remaining <= 0) {
            closeNow();
        } else {
            timer = context.executor().schedule(this::checkDrain, remaining, TimeUnit.NANOSECONDS);
        }
    }

    private void closeNow() {
        assert context.executor().inEventLoop();
        if (phase == Phase.CLOSED) {
            return;
        }
        finish();
        // starts in front of this handler: bypasses both the deferral below and SslHandler's own close handling
        context.close();
    }

    private void finish() {
        phase = Phase.CLOSED;
        pendingOperation = null;
        cancelTimer();
    }

    private void cancelTimer() {
        FutureUtils.cancel(timer);
        timer = null;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ReferenceCountUtil.release(msg);
        if (phase == Phase.DRAINING) {
            lastReadNanos = ctx.executor().ticker().nanoTime();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ChannelInputShutdownEvent) {
            // without this handler Netty would have closed the connection: it can only end once the response is out
            if (phase == Phase.DRAINING) {
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
        finish();
        ctx.fireChannelInactive();
    }
}
