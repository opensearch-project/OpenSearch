/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * An {@link HttpObjectAggregator} that makes sure the {@code 413 Request Entity Too Large} response it sends for an oversized
 * request actually reaches the client when the connection has to be closed.
 * <p>
 * Netty's aggregator writes the 413 and then fully closes the socket whenever the client may already be sending the body,
 * which is always the case for chunked or compressed requests (the decompressor drops {@code Content-Length}, so the limit
 * is only hit while the body is streaming in). The client however keeps sending the rest of its body; the server's kernel
 * answers those bytes (or the bytes still unread in the receive buffer at close time) with a TCP RST, and a RST makes the
 * client's kernel discard the 413 it has not read yet. Whether the client observes the 413 or a connection reset (which
 * load balancers turn into a 502) therefore depends on timing.
 * <p>
 * This implementation performs a <em>lingering close</em> instead, as described in RFC 9112 section 9.6: it writes the 413
 * with {@code Connection: close}, half-closes the connection (TLS {@code close_notify} followed by a TCP FIN) so the client
 * sees the complete response followed by end-of-stream, and keeps reading and discarding whatever the client still sends
 * until the client closes its side, stops sending for {@link #LINGER_IDLE_TIMEOUT_MILLIS}, or
 * {@link #LINGER_MAX_MILLIS} have passed. The discarded bytes are dropped at the very head of the pipeline, so they are
 * neither decrypted, decoded nor decompressed.
 * <p>
 * Requests rejected without the need to close the connection (a keep-alive request whose {@code Content-Length} is
 * already too large) keep Netty's behavior of answering 413 and discarding the body while keeping the connection open.
 */
class Netty4HttpObjectAggregator extends HttpObjectAggregator {

    static final long LINGER_IDLE_TIMEOUT_MILLIS = 5_000;
    static final long LINGER_MAX_MILLIS = 30_000;
    static final String LINGERING_CLOSE_HANDLER_NAME = "lingering_close";

    private static final FullHttpResponse TOO_LARGE_CLOSE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
        Unpooled.EMPTY_BUFFER
    );

    static {
        TOO_LARGE_CLOSE.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        TOO_LARGE_CLOSE.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
    }

    /** set once an oversized request has been rejected and the connection is being closed: nothing else must be processed */
    private boolean closing;

    Netty4HttpObjectAggregator(int maxContentLength, int maxCumulationBufferComponents) {
        super(maxContentLength);
        setMaxCumulationBufferComponents(maxCumulationBufferComponents);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (closing) {
            // leftovers of the rejected request (or anything pipelined after it) that were already decoded
            ReferenceCountUtil.release(msg);
            return;
        }
        super.channelRead(ctx, msg);
    }

    @Override
    protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        if (oversized instanceof HttpRequest && ctx.channel() instanceof DuplexChannel && mustCloseConnection(ctx, oversized)) {
            closing = true;
            final LingeringCloseHandler lingeringCloseHandler = LingeringCloseHandler.install(ctx.channel());
            ctx.writeAndFlush(TOO_LARGE_CLOSE.retainedDuplicate()).addListener(f -> {
                if (f.isSuccess()) {
                    lingeringCloseHandler.shutdownOutput();
                } else {
                    ctx.close();
                }
            });
        } else {
            super.handleOversizedMessage(ctx, oversized);
        }
    }

    /** Mirrors the decision {@link HttpObjectAggregator#handleOversizedMessage} makes about closing the connection. */
    private static boolean mustCloseConnection(ChannelHandlerContext ctx, HttpMessage oversized) {
        return oversized instanceof FullHttpMessage
            || ctx.channel().config().isAutoRead() == false
            || (HttpUtil.is100ContinueExpected(oversized) == false && HttpUtil.isKeepAlive(oversized) == false);
    }

    /**
     * Sits at the head of the pipeline once a request was rejected and drains the connection until it can be closed without
     * resetting the client.
     */
    static final class LingeringCloseHandler extends ChannelInboundHandlerAdapter {

        private final Channel channel;
        private final long idleTimeoutMillis;
        private final long maxLingerMillis;
        private ScheduledFuture<?> idleTimeout;
        private ScheduledFuture<?> maxLingerTimeout;
        private boolean outputShutdown;

        LingeringCloseHandler(Channel channel, long idleTimeoutMillis, long maxLingerMillis) {
            this.channel = channel;
            this.idleTimeoutMillis = idleTimeoutMillis;
            this.maxLingerMillis = maxLingerMillis;
        }

        static LingeringCloseHandler install(Channel channel) {
            final ChannelPipeline pipeline = channel.pipeline();
            final LingeringCloseHandler handler = new LingeringCloseHandler(channel, LINGER_IDLE_TIMEOUT_MILLIS, LINGER_MAX_MILLIS);
            // a read timeout would no longer see any reads and must not cut the drain short, the linger timeouts take over
            if (pipeline.get("read_timeout") != null) {
                pipeline.remove("read_timeout");
            }
            pipeline.addFirst(LINGERING_CLOSE_HANDLER_NAME, handler);
            // keep reading, otherwise the client's data piles up and closing would reset the connection anyway
            channel.config().setAutoRead(true);
            return handler;
        }

        /** Called once the 413 was written: half-close so the client sees the response followed by end-of-stream. */
        void shutdownOutput() {
            assert channel.eventLoop().inEventLoop();
            if (channel.isActive() == false) {
                return;
            }
            final SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
            if (sslHandler != null) {
                // send close_notify first so TLS clients do not see a truncated stream
                sslHandler.closeOutbound().addListener(f -> shutdownSocketOutput());
            } else {
                shutdownSocketOutput();
            }
        }

        private void shutdownSocketOutput() {
            if (channel.isActive() == false) {
                return;
            }
            outputShutdown = true;
            ((DuplexChannel) channel).shutdownOutput().addListener((ChannelFuture f) -> {
                if (f.isSuccess() == false) {
                    channel.close();
                }
            });
            maxLingerTimeout = channel.eventLoop().schedule(this::close, maxLingerMillis, TimeUnit.MILLISECONDS);
            resetIdleTimeout();
        }

        private void resetIdleTimeout() {
            if (idleTimeout != null) {
                FutureUtils.cancel(idleTimeout);
            }
            idleTimeout = channel.eventLoop().schedule(this::close, idleTimeoutMillis, TimeUnit.MILLISECONDS);
        }

        private void close() {
            cancelTimeouts();
            channel.close();
        }

        private void cancelTimeouts() {
            if (idleTimeout != null) {
                FutureUtils.cancel(idleTimeout);
            }
            if (maxLingerTimeout != null) {
                FutureUtils.cancel(maxLingerTimeout);
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ReferenceCountUtil.release(msg);
            if (outputShutdown) {
                resetIdleTimeout();
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            // swallowed: nothing downstream should react to the discarded bytes
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ChannelInputShutdownEvent) {
                // the client is done sending, the connection can now be closed without a reset
                close();
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // the client is allowed to reset the connection once it read the 413, there is nothing to report
            close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            cancelTimeouts();
            super.channelInactive(ctx);
        }
    }
}
