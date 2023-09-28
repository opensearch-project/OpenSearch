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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

/** POC for how an external header verifier would be implemented */
@ChannelHandler.Sharable
public class Netty4HeaderVerifier extends ChannelInboundHandlerAdapter {

    final static Logger log = LogManager.getLogger(Netty4HeaderVerifier.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof HttpRequest)) {
            ctx.fireChannelRead(msg);
        }

        HttpRequest request = (HttpRequest) msg;
        if (!isAuthenticated(request)) {
            final FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.UNAUTHORIZED);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            ReferenceCountUtil.release(msg);
        } else {
            // Lets the request pass to the next channel handler
            ctx.fireChannelRead(msg);
        }
    }

    private boolean isAuthenticated(HttpRequest request) {
        log.info("Checking if request is authenticated:\n" + request);

        final boolean shouldBlock = request.headers().contains("blockme");

        return !shouldBlock;
    }
}
