/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;

/**
 * Adds the declared encoded {@code Content-Length} to Netty's decoded-content limit.
 * The recorder refreshes the value for every request before decompression removes the header.
 * Unknown encoded lengths are not counted.
 */
final class Netty4HttpObjectAggregator extends HttpObjectAggregator {
    private long encodedContentLength = -1;

    Netty4HttpObjectAggregator(int maxContentLength) {
        super(maxContentLength);
    }

    ChannelHandler encodedLengthRecorder() {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof HttpRequest request) {
                    encodedContentLength = contentLength(request);
                }
                ctx.fireChannelRead(msg);
            }
        };
    }

    private static long contentLength(HttpMessage message) {
        try {
            return HttpUtil.getContentLength(message, -1L);
        } catch (NumberFormatException e) {
            return -1L; // the decoder rejects the request itself
        }
    }

    @Override
    protected boolean isContentLengthInvalid(HttpMessage start, int maxContentLength) {
        return super.isContentLengthInvalid(start, maxContentLength) || encodedContentLength > maxContentLength;
    }

    @Override
    protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        if (encodedContentLength > maxContentLength() && super.isContentLengthInvalid(oversized, maxContentLength()) == false) {
            // Make Netty's rejection terminal so the upload is discarded before decompression.
            // Expect rejections are handled earlier by Netty's newContinueResponse and remain recoverable.
            HttpUtil.setKeepAlive(oversized, false);
        }
        super.handleOversizedMessage(ctx, oversized);
    }
}
