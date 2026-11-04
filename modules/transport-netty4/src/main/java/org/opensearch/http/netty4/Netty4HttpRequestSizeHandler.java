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
import java.util.function.BooleanSupplier;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Protects a terminal size rejection from subsequent input and closes its output before draining the socket.
 * This handler precedes HTTP decoding so rejected uploads no longer consume decompression or aggregation work.
 */
final class Netty4HttpRequestSizeHandler extends ChannelDuplexHandler {
    private static final int DRAIN_TIMEOUT_SECONDS = 1;

    private final BooleanSupplier transportStarted;
    private ChannelHandlerContext context;
    private boolean rejecting;
    private int pendingResponses;
    private boolean closeRequested;
    private ScheduledFuture<?> drainTimeout;

    Netty4HttpRequestSizeHandler(Netty4HttpServerTransport transport) {
        transportStarted = () -> transport.lifecycleState() == Lifecycle.State.STARTED;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        context = ctx;
    }

    void reject(boolean close) {
        pendingResponses++;
        rejecting |= close;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (rejecting) {
            ReferenceCountUtil.release(msg);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        if ((rejecting || pendingResponses > 0) && transportStarted.getAsBoolean()) {
            // A decoder failure or read timeout must not close the socket ahead of the rejection write.
            closeRequested = true;
            ctx.channel().closeFuture().addListener(future -> promise.trySuccess());
        } else {
            ctx.close(promise);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        ctx.fireUserEventTriggered(event);
        if (event instanceof ChannelInputShutdownEvent) {
            closeRequested = true;
            if (pendingResponses == 0) {
                ctx.close();
            }
        }
    }

    void responseWritten(Future<?> future) {
        if (future.isSuccess() == false) {
            context.close();
            return;
        }
        if (--pendingResponses > 0 || (rejecting == false && closeRequested == false)) {
            return;
        }
        rejecting = true;
        SslHandler ssl = context.pipeline().get(SslHandler.class);
        if (ssl == null) {
            shutdownOutput();
        } else {
            // Flush TLS close_notify after the response, before shutting down the underlying TCP output.
            ssl.closeOutbound().addListener(closed -> {
                if (closed.isSuccess()) {
                    shutdownOutput();
                } else {
                    context.close();
                }
            });
        }
    }

    private void shutdownOutput() {
        if (context.channel() instanceof DuplexChannel channel) {
            if (channel.isInputShutdown()) {
                context.close();
                return;
            }
            channel.shutdownOutput().addListener(future -> {
                if (future.isSuccess()) {
                    // Closing with unread input can send RST and discard the response at a proxy.
                    // Keep consuming input after FIN, bounded in case the sender never stops uploading.
                    drainTimeout = context.executor().schedule(() -> context.close(), DRAIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } else {
                    context.close();
                }
            });
        } else {
            context.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (drainTimeout != null) {
            FutureUtils.cancel(drainTimeout);
        }
        ctx.fireChannelInactive();
    }
}
