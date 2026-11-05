/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import java.util.function.Consumer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;

/**
 * Bounds encoded request bodies before decompression. The aggregator independently bounds decoded bodies.
 * Counts only content bytes, excluding HTTP headers and transfer framing, without retaining any buffers.
 */
final class Netty4HttpEncodedSizeHandler extends ChannelInboundHandlerAdapter {
    private final int maxContentLength;
    private final Consumer<ChannelHandlerContext> reject;
    private int remaining;
    private boolean encoded;
    private boolean rejected;

    Netty4HttpEncodedSizeHandler(int maxContentLength, Consumer<ChannelHandlerContext> reject) {
        this.maxContentLength = maxContentLength;
        this.reject = reject;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (rejected) {
            ReferenceCountUtil.release(msg);
            return;
        }
        if (msg instanceof HttpRequest request) {
            remaining = maxContentLength;
            // Match HttpContentDecoder's choice of content coding, including transfer compression.
            String encoding = request.headers().get(HttpHeaderNames.CONTENT_ENCODING);
            if (encoding == null) {
                encoding = request.headers().get(HttpHeaderNames.TRANSFER_ENCODING);
                if (encoding != null && encoding.indexOf(',') >= 0) {
                    encoding = encoding.substring(0, encoding.indexOf(','));
                }
            }
            encoded = request.decoderResult().isSuccess()
                && encoding != null
                && HttpHeaderValues.IDENTITY.contentEqualsIgnoreCase(encoding.trim()) == false
                && HttpHeaderValues.CHUNKED.contentEqualsIgnoreCase(encoding.trim()) == false;
            // Decompression removes Content-Length, so reject known oversized encoded bodies before that happens.
            if (encoded && HttpUtil.getContentLength(request, -1L) > maxContentLength) {
                reject(ctx, msg);
                return;
            }
        }
        if (encoded && msg instanceof HttpContent content) {
            int size = content.content().readableBytes();
            if (size > remaining) {
                reject(ctx, msg);
                return;
            }
            remaining -= size;
        }
        ctx.fireChannelRead(msg);
    }

    private void reject(ChannelHandlerContext ctx, Object msg) {
        rejected = true;
        ReferenceCountUtil.release(msg);
        reject.accept(ctx);
    }
}
