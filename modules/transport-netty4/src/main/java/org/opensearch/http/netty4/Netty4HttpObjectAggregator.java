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
 * Preserves recoverable HTTP/1 rejections and protects terminal rejections until the response has been written.
 */
final class Netty4HttpObjectAggregator extends HttpObjectAggregator {
    private final Netty4HttpServerTransport transport;
    private Netty4HttpRejectionHandler rejection;

    Netty4HttpObjectAggregator(int maxContentLength, Netty4HttpServerTransport transport) {
        super(maxContentLength);
        this.transport = transport;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // A decoder can emit several requests from one read. They have already passed the raw-input discard handler.
        if (rejection != null && rejection.isClosing()) {
            ReferenceCountUtil.release(msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    protected Object newContinueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline) {
        Object response = super.newContinueResponse(start, maxContentLength, pipeline);
        if (response instanceof HttpResponse httpResponse && httpResponse.status().equals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)) {
            // The superclass also resets the decoder after a failed expectation. Returning null makes MessageAggregator
            // check Content-Length immediately, routing the rejection through the protected write below.
            ReferenceCountUtil.release(response);
            return null;
        }
        return response;
    }

    @Override
    protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        if (oversized instanceof HttpRequest) {
            // Match Netty's decision about whether the connection can be reused.
            boolean close = oversized instanceof FullHttpMessage
                || ctx.channel().config().isAutoRead() == false
                || (HttpUtil.is100ContinueExpected(oversized) == false && HttpUtil.isKeepAlive(oversized) == false);
            reject(ctx, close);
        } else {
            super.handleOversizedMessage(ctx, oversized);
        }
    }

    void reject(ChannelHandlerContext ctx, boolean close) {
        if (rejection != null && rejection.isClosing()) {
            return;
        }
        if (rejection == null || rejection.isRemoved()) {
            rejection = new Netty4HttpRejectionHandler(transport);
            ctx.pipeline().addFirst("http_rejection", rejection);
        }
        rejection.reject(close);
        if (close) {
            // An encoded-size rejection can arrive while decoded content is still being accumulated.
            releaseCurrentMessage();
        }
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
            Unpooled.EMPTY_BUFFER
        );
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        if (close) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        }
        ctx.writeAndFlush(response).addListener(rejection::responseWritten);
    }
}
