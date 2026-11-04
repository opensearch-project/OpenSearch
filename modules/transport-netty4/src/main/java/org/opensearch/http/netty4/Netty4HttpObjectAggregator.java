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
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;

/**
 * Preserves Netty's recoverable rejections and flushes terminal HTTP/1 rejections before closing the connection.
 */
final class Netty4HttpObjectAggregator extends HttpObjectAggregator {
    Netty4HttpObjectAggregator(int maxContentLength) {
        super(maxContentLength);
    }

    @Override
    protected Object newContinueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline) {
        Object response = super.newContinueResponse(start, maxContentLength, pipeline);
        if (response instanceof HttpResponse httpResponse && httpResponse.status().equals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)) {
            // Route an oversized Expect request through handleOversizedMessage too, so its write is protected.
            ReferenceCountUtil.release(response);
            start.headers().remove(HttpHeaderNames.EXPECT);
            return null;
        }
        return response;
    }

    @Override
    protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        if (oversized instanceof HttpRequest) {
            boolean close = oversized instanceof FullHttpMessage
                || ctx.channel().config().isAutoRead() == false
                || (HttpUtil.is100ContinueExpected(oversized) == false && HttpUtil.isKeepAlive(oversized) == false);
            Netty4HttpRequestSizeHandler sizeHandler = ctx.pipeline().get(Netty4HttpRequestSizeHandler.class);
            sizeHandler.reject(close);
            FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                Unpooled.EMPTY_BUFFER
            );
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            if (close) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            }
            ctx.writeAndFlush(response).addListener(sizeHandler::responseWritten);
        } else {
            super.handleOversizedMessage(ctx, oversized);
        }
    }
}
