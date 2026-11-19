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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Delivers a terminal HTTP/1 {@code 413} before closing the connection.
 * <p>
 * The aggregator overrides invoke {@link #writeAndClose(ChannelHandlerContext, FullHttpResponse)} for terminal rejections.
 * This handler is installed before TLS and HTTP decoding, leaving Netty's size-limit decisions unchanged.
 * It discards remaining input, defers pipeline closes, and enables input half-closure so a client FIN cannot interrupt delivery.
 * <p>
 * Response write, TLS {@code close_notify}, and TCP output shutdown run in order under one hard {@link #WRITE_TIMEOUT_NANOS}
 * deadline, including their completion callbacks. The connection then drains until peer EOF, {@link #DRAIN_IDLE_NANOS}
 * of inactivity, or {@link #DRAIN_MAX_NANOS} total. Draining avoids a TCP reset from unread input discarding the response,
 * as described in RFC 9112 section 9.6.
 */
final class Netty4HttpRejectionHandler extends ChannelDuplexHandler {

    static final String NAME = "http_rejection";
    private static final Logger logger = LogManager.getLogger(Netty4HttpRejectionHandler.class);

    private static final long WRITE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    // Match the defaults of NGINX's lingering_timeout and lingering_time.
    private static final long DRAIN_IDLE_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final long DRAIN_MAX_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final BooleanSupplier transportRunning;
    private ChannelHandlerContext context;
    private String operation;
    private boolean draining;
    private boolean closed;
    private ScheduledFuture<?> timer;

    Netty4HttpRejectionHandler(BooleanSupplier transportRunning) {
        this.transportRunning = transportRunning;
    }

    /** Shares rejection state across the HTTP stages while the installed handler drains raw input. */
    ChannelHandler newHttpHandler() {
        return new ChannelInboundHandlerAdapter() {
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

    ChannelFuture writeAndClose(ChannelHandlerContext ctx, FullHttpResponse response) {
        final ChannelPromise promise = ctx.newPromise();
        reject((DuplexChannel) ctx.channel(), promise);
        return ctx.writeAndFlush(response, promise);
    }

    /** Installs close protection before the terminal response is written. Its promise bounds the delivery lifecycle. */
    void reject(DuplexChannel channel, ChannelPromise response) {
        assert channel.eventLoop().inEventLoop();
        channel.pipeline().addFirst(NAME, this);
        timer = channel.eventLoop().schedule(() -> {
            logger.debug("closing {} after the 413 write deadline expired during {}", channel, operation);
            closeNow();
        }, WRITE_TIMEOUT_NANOS, TimeUnit.NANOSECONDS);
        await("response write", response, this::shutdownTls);
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

    private void await(String operation, ChannelFuture future, Runnable next) {
        this.operation = operation;
        future.addListener(completed -> {
            assert context.executor().inEventLoop();
            if (closed) {
                return;
            }
            if (completed.isSuccess() && context.channel().isActive()) {
                next.run();
            } else {
                logger.debug("closing {} during {} after a 413", context.channel(), operation, completed.cause());
                closeNow();
            }
        });
    }

    private void shutdownTls() {
        final SslHandler ssl = context.pipeline().get(SslHandler.class);
        if (ssl == null) {
            shutdownOutput();
        } else {
            await("TLS shutdown", ssl.closeOutbound(), this::shutdownOutput);
        }
    }

    private void shutdownOutput() {
        await("TCP output shutdown", ((DuplexChannel) context.channel()).shutdownOutput(), this::drain);
    }

    private void drain() {
        draining = true;
        operation = "input drain";
        cancelTimer();
        if (((DuplexChannel) context.channel()).isInputShutdown()) {
            closeNow();
        } else {
            // Observe raw input before this handler discards it, and let Netty close on inactivity.
            context.pipeline().addBefore(NAME, null, new ReadTimeoutHandler(DRAIN_IDLE_NANOS, TimeUnit.NANOSECONDS));
            timer = context.executor().schedule(() -> {
                logger.debug("closing {} after the 413 drain total timeout", context.channel());
                closeNow();
            }, DRAIN_MAX_NANOS, TimeUnit.NANOSECONDS);
        }
    }

    private void closeNow() {
        assert context.executor().inEventLoop();
        if (closed) {
            return;
        }
        finish();
        // starts in front of this handler: bypasses both the deferral below and SslHandler's own close handling
        context.close();
    }

    private void finish() {
        closed = true;
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
            if (draining) {
                closeNow();
            }
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        // Socket errors and the drain's idle timeout arrive here; the HTTP guards handle decoding errors.
        logger.debug("closing {} after an exception during the 413 {}", ctx.channel(), operation, cause);
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
