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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DuplexChannel;
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
 * The HTTP/1 {@link HttpObjectAggregator} of {@link Netty4HttpServerTransport}.
 * <p>
 * A request is too large when either its body as sent or its decoded (decompressed) body exceeds
 * {@code http.max_content_length}, whichever is detected first. The decoded size is enforced while the body streams in,
 * as Netty does. The size as sent is checked up front from the request's {@code Content-Length}, which the content
 * decompressor removes from compressed requests before they reach this aggregator; {@link #encodedLengthRecorder()}
 * captures it ahead of the decompressor. This lets an oversized compressed request be rejected before its body is sent,
 * including when the client asked for {@code Expect: 100-continue}.
 * <p>
 * Rejections are handed to a {@link Netty4HttpRejectionHandler} so the {@code 413} reaches the client. The connection is
 * kept open when Netty would keep it open: the request was rejected before any body was read and it is keep-alive, with
 * the exception of a compressed body rejected on its declared size without {@code 100-continue}, whose upload is
 * imminent and would otherwise have to be decompressed only to be discarded.
 */
final class Netty4HttpObjectAggregator extends HttpObjectAggregator {

    private static final Logger logger = LogManager.getLogger(Netty4HttpObjectAggregator.class);

    private final BooleanSupplier transportRunning;
    private final Netty4HttpRejectionHandler.Timeouts timeouts;
    /** {@code Content-Length} of the current request as sent, recorded ahead of the decompressor; -1 when unknown */
    private long encodedContentLength = -1;
    /** the current request asked for {@code 100-continue} and was rejected on its declared size */
    private boolean expectationRejected;
    private Netty4HttpRejectionHandler rejection;

    Netty4HttpObjectAggregator(
        int maxContentLength,
        int maxCumulationBufferComponents,
        BooleanSupplier transportRunning,
        Netty4HttpRejectionHandler.Timeouts timeouts
    ) {
        super(maxContentLength);
        setMaxCumulationBufferComponents(maxCumulationBufferComponents);
        this.transportRunning = transportRunning;
        this.timeouts = timeouts;
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

    private boolean isTerminal() {
        return rejection != null && rejection.isTerminal();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (isTerminal()) {
            // decoded from input that was read before the rejection, e.g. pipelined requests in the same buffer
            ReferenceCountUtil.release(msg);
            return;
        }
        final boolean start = msg instanceof HttpMessage;
        if (start) {
            expectationRejected = false;
        }
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
        if (isTerminal()) {
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
    protected Object newContinueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline) {
        final Object response = super.newContinueResponse(start, maxContentLength, pipeline);
        if (response instanceof HttpResponse httpResponse && httpResponse.status().equals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)) {
            // Netty would write its 413 here, unprotected. Returning null makes the aggregator check the content length
            // next, which routes the rejection through handleOversizedMessage below. The decoder was already reset.
            ReferenceCountUtil.release(response);
            expectationRejected = true;
            return null;
        }
        return response;
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
            || (rejectedOnEncodedLengthOnly && expectationRejected == false);

        rejection = Netty4HttpRejectionHandler.install(ctx.pipeline(), transportRunning, timeouts);
        final ChannelPromise promise = ctx.newPromise();
        rejection.responsePending(close, promise);
        final FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
            Unpooled.EMPTY_BUFFER
        );
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        if (close) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        }
        ctx.writeAndFlush(response, promise);
    }
}
