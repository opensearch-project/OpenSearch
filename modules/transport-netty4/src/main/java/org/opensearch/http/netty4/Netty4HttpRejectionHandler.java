/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Installed before TLS and HTTP decoding only while rejecting a request. A successful recoverable rejection removes this
 * handler. A terminal rejection flushes the response and TLS close_notify, shuts down output, then drains raw input.
 * A completed write does not mean the peer has read the response: immediately closing with unread input can reset the
 * connection and lose that response. This is the staged shutdown described in RFC 9112 section 9.6.
 * One timer bounds writes/TLS shutdown, then checks drain idleness and a maximum drain duration. Reads only update a
 * timestamp, avoiding timer cancellation and allocation for every discarded buffer.
 */
final class Netty4HttpRejectionHandler extends ChannelDuplexHandler {
    static final int WRITE_TIMEOUT_SECONDS = 30;
    static final int DRAIN_IDLE_SECONDS = 5;
    static final int DRAIN_MAX_SECONDS = 30;

    private final Netty4HttpServerTransport transport;
    private ChannelHandlerContext context;
    private boolean allowHalfClosure;
    private boolean closing;
    private boolean outputShutdown;
    private int pendingResponses;
    private ScheduledFuture<?> timeout;
    private long lastReadNanos;
    private long drainDeadlineNanos;

    Netty4HttpRejectionHandler(Netty4HttpServerTransport transport) {
        this.transport = transport;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        context = ctx;
        if (ctx.channel().config() instanceof SocketChannelConfig config) {
            allowHalfClosure = config.isAllowHalfClosure();
            config.setAllowHalfClosure(true);
        }
        scheduleTimeout(WRITE_TIMEOUT_SECONDS);
    }

    boolean isClosing() {
        return closing;
    }

    boolean isRemoved() {
        return context.isRemoved();
    }

    void reject(boolean close) {
        pendingResponses++;
        if (close) {
            startClosing();
        }
    }

    private void startClosing() {
        closing = true;
        // Otherwise unread upload data can accumulate until the final close resets the connection.
        context.channel().config().setAutoRead(true);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (closing) {
            ReferenceCountUtil.release(msg);
            if (outputShutdown) {
                lastReadNanos = ctx.executor().ticker().nanoTime();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        if (transport.lifecycleState() == Lifecycle.State.STARTED) {
            // Decoder errors and read timeouts must not overtake the response. The deadline still bounds this wait.
            startClosing();
            ctx.channel().closeFuture().addListener(future -> promise.trySuccess());
        } else {
            ctx.close(promise);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof ChannelInputShutdownEvent) {
            startClosing();
            if (outputShutdown) {
                closeNow();
            }
        }
        ctx.fireUserEventTriggered(event);
    }

    void responseWritten(Future<?> future) {
        if (future.isSuccess() == false) {
            closeNow();
        } else if (--pendingResponses == 0 && context.channel().isActive()) {
            if (closing == false) {
                // Restore the normal connection behavior, including Netty's handling of input EOF.
                if (context.channel().config() instanceof SocketChannelConfig config) {
                    config.setAllowHalfClosure(allowHalfClosure);
                }
                context.pipeline().remove(this);
                return;
            }
            SslHandler ssl = context.pipeline().get(SslHandler.class);
            if (ssl == null) {
                shutdownOutput();
            } else {
                ssl.closeOutbound().addListener(closed -> {
                    if (closed.isSuccess()) {
                        shutdownOutput();
                    } else {
                        closeNow();
                    }
                });
            }
        }
    }

    private void shutdownOutput() {
        if (context.channel().isActive() == false) {
            return;
        }
        if (context.channel() instanceof DuplexChannel channel && channel.isInputShutdown() == false) {
            channel.shutdownOutput().addListener(future -> {
                if (future.isSuccess() && channel.isActive()) {
                    outputShutdown = true;
                    lastReadNanos = context.executor().ticker().nanoTime();
                    drainDeadlineNanos = lastReadNanos + TimeUnit.SECONDS.toNanos(DRAIN_MAX_SECONDS);
                    checkDrain();
                } else {
                    closeNow();
                }
            });
        } else {
            closeNow();
        }
    }

    private void scheduleTimeout(int seconds) {
        FutureUtils.cancel(timeout);
        timeout = context.executor().schedule(this::closeNow, seconds, TimeUnit.SECONDS);
    }

    private void checkDrain() {
        FutureUtils.cancel(timeout);
        long now = context.executor().ticker().nanoTime();
        long remaining = Math.min(drainDeadlineNanos - now, lastReadNanos + TimeUnit.SECONDS.toNanos(DRAIN_IDLE_SECONDS) - now);
        if (remaining <= 0) {
            closeNow();
        } else {
            timeout = context.executor().schedule(this::checkDrain, remaining, TimeUnit.NANOSECONDS);
        }
    }

    private void closeNow() {
        closing = true;
        // Start before this handler to bypass our close deferral (and TLS's own close handling).
        context.close();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        FutureUtils.cancel(timeout);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        FutureUtils.cancel(timeout);
        ctx.fireChannelInactive();
    }
}
