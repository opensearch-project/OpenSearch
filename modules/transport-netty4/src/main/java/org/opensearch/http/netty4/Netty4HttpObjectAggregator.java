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

import java.util.function.BooleanSupplier;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;

/**
 * The HTTP/1 {@link HttpObjectAggregator} of {@link Netty4HttpServerTransport}.
 * <p>
 * Preserves Netty's size-limit decisions, including its decoded-content limit for compressed requests. Rejections
 * that close the connection use a {@link Netty4HttpRejectionHandler} so the {@code 413} reaches the client.
 * Netty handles recoverable rejections, including rejected {@code Expect: 100-continue} requests.
 */
final class Netty4HttpObjectAggregator extends HttpObjectAggregator {

    private static final Logger logger = LogManager.getLogger(Netty4HttpObjectAggregator.class);

    private final BooleanSupplier transportRunning;
    private boolean rejecting;

    Netty4HttpObjectAggregator(int maxContentLength, int maxCumulationBufferComponents, BooleanSupplier transportRunning) {
        super(maxContentLength);
        setMaxCumulationBufferComponents(maxCumulationBufferComponents);
        this.transportRunning = transportRunning;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (rejecting) {
            // decoded from input that was read before the rejection, e.g. pipelined requests in the same buffer
            ReferenceCountUtil.release(msg);
            return;
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (rejecting) {
            // e.g. a decompression failure on the remainder of the rejected body: expected, and closing is under way
            logger.debug("ignoring exception on a connection that is closing after a 413", cause);
            return;
        }
        super.exceptionCaught(ctx, cause);
    }

    @Override
    protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        if (oversized instanceof HttpRequest == false || ctx.channel() instanceof DuplexChannel == false) {
            super.handleOversizedMessage(ctx, oversized);
            return;
        }
        // Netty handles header-only Expect rejections in newContinueResponse before reaching this method.
        final boolean close = oversized instanceof FullHttpMessage // part of the body was read already
            || ctx.channel().config().isAutoRead() == false
            || HttpUtil.isKeepAlive(oversized) == false;
        if (close == false) {
            super.handleOversizedMessage(ctx, oversized);
            return;
        }

        rejecting = true;
        Netty4HttpRejectionHandler.reject(ctx, transportRunning);
    }
}
