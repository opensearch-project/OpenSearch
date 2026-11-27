/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Delivers a terminal HTTP/1 {@code 413} before closing the connection.
 * <p>
 * The HTTP handlers from {@link #newHttpHandler()} observe full {@code 413} responses without keep-alive.
 * This includes closing application responses, such as HTTP/1.0 errors, without changing Netty's size-limit decisions.
 * On a terminal rejection, this handler is installed before TLS and HTTP decoding.
 * It discards remaining input, defers pipeline closes, and enables input half-closure so a client FIN cannot interrupt delivery.
 * <p>
 * Response write, TLS {@code close_notify}, and TCP output shutdown run in order under one {@link #WRITE_TIMEOUT_NANOS}
 * deadline. The connection then drains until peer EOF, {@link #DRAIN_IDLE_NANOS} of inactivity, or {@link #DRAIN_MAX_NANOS}
 * total. Draining avoids a TCP reset from unread input discarding the response, as described in RFC 9112 section 9.6.
 */
final class Netty4HttpRejectionHandler extends ChannelDuplexHandler {

    static final String NAME = "http_rejection";
    private static final Logger logger = LogManager.getLogger(Netty4HttpRejectionHandler.class);

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
    private ScheduledFuture<?> timer;

    Netty4HttpRejectionHandler(BooleanSupplier transportRunning) {
        this.transportRunning = transportRunning;
    }

    /** Shares rejection state across the HTTP stages while the installed handler drains raw input. */
    ChannelHandler newHttpHandler() {
        return new ChannelDuplexHandler() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                if (context == null
                    && msg instanceof FullHttpResponse response
                    && response.status().equals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)
                    && HttpUtil.isKeepAlive(response) == false) {
                    promise = promise.unvoid();
                    reject((DuplexChannel) ctx.channel(), promise);
                }
                ctx.write(msg, promise);
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (context == null) {
                    ctx.fireChannelRead(msg);
                } else {
                    // Already decoded from input read before the rejection, including pipelined requests.
                    ReferenceCountUtil.release(msg);
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                if (context == null) {
                    ctx.fireExceptionCaught(cause);
                } else {
                    ExceptionsHelper.maybeDieOnAnotherThread(cause);
                    logger.debug("ignoring exception on a connection that is closing after a 413", cause);
                }
            }
        };
    }

    /** Installs close protection before the terminal response is written. Its promise bounds the delivery lifecycle. */
    void reject(DuplexChannel channel, ChannelPromise response) {
        assert channel.eventLoop().inEventLoop();
        channel.pipeline().addFirst(NAME, this);
        timer = context.executor().schedule(this::writeTimedOut, WRITE_TIMEOUT_NANOS, TimeUnit.NANOSECONDS);
        await(Phase.RESPONSE, response);
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

    private void writeTimedOut() {
        assert context.executor().inEventLoop();
        // Netty may defer promise listeners to avoid excessive listener recursion. Process completed
        // operations before deciding whether the write, TLS shutdown or FIN has timed out.
        while (pendingOperation != null && pendingOperation.isDone()) {
            operationCompleted(pendingOperation);
        }
        if (phase != Phase.DRAINING && phase != Phase.CLOSED) {
            logger.debug("closing {} after the 413 write deadline expired during {}", context.channel(), phase);
            closeNow();
        }
    }

    private void await(Phase nextPhase, ChannelFuture future) {
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
            logger.debug("closing {} during {} after a 413", context.channel(), phase, future.cause());
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
                cancelTimer();
                if (((DuplexChannel) context.channel()).isInputShutdown()) {
                    closeNow();
                } else {
                    // Observe raw input before this handler discards it.
                    context.pipeline().addBefore(NAME, null, new IdleStateHandler(DRAIN_IDLE_NANOS, 0, 0, TimeUnit.NANOSECONDS));
                    timer = context.executor().schedule(() -> drainTimedOut("total"), DRAIN_MAX_NANOS, TimeUnit.NANOSECONDS);
                }
            }
            default -> throw new IllegalStateException("Unexpected rejection phase: " + phase);
        }
    }

    private void shutdownOutput() {
        await(Phase.OUTPUT_SHUTDOWN, ((DuplexChannel) context.channel()).shutdownOutput());
    }

    private void drainTimedOut(String bound) {
        logger.debug("closing {} after the 413 drain {} timeout", context.channel(), bound);
        closeNow();
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
        if (evt instanceof IdleStateEvent) {
            drainTimedOut("idle");
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        // Only the socket itself reports here (this handler is first); a peer may reset once it read the response.
        logger.debug("closing {} after a socket exception during a 413 rejection", ctx.channel(), cause);
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
