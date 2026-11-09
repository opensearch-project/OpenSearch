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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
 * A request is too large when either its body as sent or its decoded (decompressed) body exceeds
 * {@code http.max_content_length}, whichever is detected first. The decoded size is enforced while the body streams in,
 * as Netty does. The size as sent is checked up front from the request's {@code Content-Length}, which the content
 * decompressor removes from compressed requests before they reach this aggregator; {@link #encodedLengthRecorder()}
 * captures it ahead of the decompressor. This lets an oversized compressed request be rejected before its body is sent,
 * including when the client asked for {@code Expect: 100-continue}.
 * <p>
 * Rejections that close the connection use a {@link Netty4HttpRejectionHandler} so the {@code 413} reaches the client.
 * Netty handles recoverable rejections, including {@code Expect: 100-continue}. A compressed body rejected on its
 * declared size without {@code 100-continue} closes the connection so its imminent upload can be discarded before
 * decompression.
 */
final class Netty4HttpObjectAggregator extends HttpObjectAggregator {

    private static final Logger logger = LogManager.getLogger(Netty4HttpObjectAggregator.class);

    private final BooleanSupplier transportRunning;
    /** {@code Content-Length} of the current request as sent, recorded ahead of the decompressor; -1 when unknown */
    private long encodedContentLength = -1;
    private boolean rejecting;

    Netty4HttpObjectAggregator(int maxContentLength, int maxCumulationBufferComponents, BooleanSupplier transportRunning) {
        super(maxContentLength);
        setMaxCumulationBufferComponents(maxCumulationBufferComponents);
        this.transportRunning = transportRunning;
    }

    /** A handler to install ahead of the content decompressor; it records the {@code Content-Length} of each request. */
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
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (rejecting) {
            // decoded from input that was read before the rejection, e.g. pipelined requests in the same buffer
            ReferenceCountUtil.release(msg);
            return;
        }
        final boolean start = msg instanceof HttpMessage;
        try {
            super.channelRead(ctx, msg);
        } finally {
            if (start) {
                encodedContentLength = -1;
            }
        }
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
    protected boolean isContentLengthInvalid(HttpMessage start, int maxContentLength) {
        return super.isContentLengthInvalid(start, maxContentLength) || encodedContentLength > maxContentLength;
    }

    @Override
    protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        if (oversized instanceof HttpRequest == false || ctx.channel() instanceof DuplexChannel == false) {
            super.handleOversizedMessage(ctx, oversized);
            return;
        }
        final boolean rejectedOnEncodedLengthOnly = encodedContentLength > maxContentLength()
            && super.isContentLengthInvalid(oversized, maxContentLength()) == false;
        final boolean close = oversized instanceof FullHttpMessage // part of the body was read already
            || ctx.channel().config().isAutoRead() == false
            || HttpUtil.isKeepAlive(oversized) == false
            || rejectedOnEncodedLengthOnly;
        if (close == false) {
            super.handleOversizedMessage(ctx, oversized);
            return;
        }

        rejecting = true;
        Netty4HttpRejectionHandler.reject(ctx, transportRunning);
    }
}
