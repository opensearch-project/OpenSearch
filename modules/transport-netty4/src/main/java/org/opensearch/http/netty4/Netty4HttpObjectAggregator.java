/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

/** Keeps Netty's aggregation and recoverable rejections, replacing only terminal request shutdown. */
final class Netty4HttpObjectAggregator extends HttpObjectAggregator {
    private final Netty4HttpRejectionHandler rejection;

    Netty4HttpObjectAggregator(int maxContentLength, Netty4HttpRejectionHandler rejection) {
        super(maxContentLength);
        this.rejection = rejection;
    }

    @Override
    protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        if (rejectIfTerminal(ctx, oversized, rejection) == false) {
            super.handleOversizedMessage(ctx, oversized);
        }
    }

    static HttpServerUpgradeHandler newUpgradeHandler(
        HttpServerCodec codec,
        HttpServerUpgradeHandler.UpgradeCodecFactory factory,
        int maxContentLength,
        Netty4HttpRejectionHandler rejection
    ) {
        return new HttpServerUpgradeHandler(codec, factory, maxContentLength) {
            @Override
            protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
                if (rejectIfTerminal(ctx, oversized, rejection) == false) {
                    super.handleOversizedMessage(ctx, oversized);
                }
            }
        };
    }

    private static boolean rejectIfTerminal(ChannelHandlerContext ctx, HttpMessage oversized, Netty4HttpRejectionHandler rejection) {
        // Copyright 2012 The Netty Project, licensed under the Apache License 2.0.
        // Predicate and response from Netty 4.2.20.Final HttpObjectAggregator.handleOversizedMessage:
        // https://github.com/netty/netty/blob/netty-4.2.20.Final/codec-http/src/main/java/io/netty/handler/codec/http/HttpObjectAggregator.java#L241-L267
        // Replace its write-then-close with bounded half-close and drain; delegate all other cases to Netty.
        if (oversized instanceof HttpRequest
            && (oversized instanceof FullHttpMessage
                || ctx.channel().config().isAutoRead() == false
                || HttpUtil.is100ContinueExpected(oversized) == false && HttpUtil.isKeepAlive(oversized) == false)) {
            final FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                Unpooled.EMPTY_BUFFER
            );
            HttpUtil.setContentLength(response, 0);
            HttpUtil.setKeepAlive(response, false);
            rejection.writeAndClose(ctx, response);
            return true;
        }
        return false;
    }
}
