/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.util.AttributeKey;

import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.tasks.Task;
import org.opensearch.transport.NettyAllocator;

import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.fail;

/**
 * Tiny helper to send http requests over netty.
 */
class Netty4HttpClient implements Closeable {

    static Collection<String> returnHttpResponseBodies(Collection<FullHttpResponse> responses) {
        List<String> list = new ArrayList<>(responses.size());
        for (FullHttpResponse response : responses) {
            list.add(response.content().toString(StandardCharsets.UTF_8));
        }
        return list;
    }

    static Collection<String> returnOpaqueIds(Collection<FullHttpResponse> responses) {
        List<String> list = new ArrayList<>(responses.size());
        for (HttpResponse response : responses) {
            list.add(response.headers().get(Task.X_OPAQUE_ID));
        }
        return list;
    }

    private final Bootstrap clientBootstrap;
    private final BiFunction<CountDownLatch, Collection<FullHttpResponse>, AwaitableChannelInitializer> handlerFactory;

    Netty4HttpClient(
        Bootstrap clientBootstrap,
        BiFunction<CountDownLatch, Collection<FullHttpResponse>, AwaitableChannelInitializer> handlerFactory
    ) {
        this.clientBootstrap = clientBootstrap;
        this.handlerFactory = handlerFactory;
    }

    static Netty4HttpClient http() {
        return new Netty4HttpClient(
            new Bootstrap().channel(NettyAllocator.getChannelType())
                .option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator())
                .group(new NioEventLoopGroup(1)),
            CountDownLatchHandlerHttp::new
        );
    }

    static Netty4HttpClient http2() {
        return new Netty4HttpClient(
            new Bootstrap().channel(NettyAllocator.getChannelType())
                .option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator())
                .group(new NioEventLoopGroup(1)),
            CountDownLatchHandlerHttp2::new
        );
    }

    public List<FullHttpResponse> get(SocketAddress remoteAddress, String... uris) throws InterruptedException {
        List<HttpRequest> requests = new ArrayList<>(uris.length);
        for (int i = 0; i < uris.length; i++) {
            final HttpRequest httpRequest = new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, uris[i]);
            httpRequest.headers().add(HOST, "localhost");
            httpRequest.headers().add("X-Opaque-ID", String.valueOf(i));
            httpRequest.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), "http");
            requests.add(httpRequest);
        }
        return sendRequests(remoteAddress, requests);
    }

    public final Collection<FullHttpResponse> post(SocketAddress remoteAddress, List<Tuple<String, CharSequence>> urisAndBodies)
        throws InterruptedException {
        return processRequestsWithBody(HttpMethod.POST, remoteAddress, urisAndBodies);
    }

    public final FullHttpResponse send(SocketAddress remoteAddress, FullHttpRequest httpRequest) throws InterruptedException {
        List<FullHttpResponse> responses = sendRequests(remoteAddress, Collections.singleton(httpRequest));
        assert responses.size() == 1 : "expected 1 and only 1 http response";
        return responses.get(0);
    }

    public final Collection<FullHttpResponse> put(SocketAddress remoteAddress, List<Tuple<String, CharSequence>> urisAndBodies)
        throws InterruptedException {
        return processRequestsWithBody(HttpMethod.PUT, remoteAddress, urisAndBodies);
    }

    private List<FullHttpResponse> processRequestsWithBody(
        HttpMethod method,
        SocketAddress remoteAddress,
        List<Tuple<String, CharSequence>> urisAndBodies
    ) throws InterruptedException {
        List<HttpRequest> requests = new ArrayList<>(urisAndBodies.size());
        for (int i = 0; i < urisAndBodies.size(); ++i) {
            final Tuple<String, CharSequence> uriAndBody = urisAndBodies.get(i);
            ByteBuf content = Unpooled.copiedBuffer(uriAndBody.v2(), StandardCharsets.UTF_8);
            HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uriAndBody.v1(), content);
            request.headers().add(HttpHeaderNames.HOST, "localhost");
            request.headers().add(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
            request.headers().add(HttpHeaderNames.CONTENT_TYPE, "application/json");
            request.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), "http");
            request.headers().add("X-Opaque-ID", String.valueOf(i));
            requests.add(request);
        }
        return sendRequests(remoteAddress, requests);
    }

    private synchronized List<FullHttpResponse> sendRequests(final SocketAddress remoteAddress, final Collection<HttpRequest> requests)
        throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(requests.size());
        final List<FullHttpResponse> content = Collections.synchronizedList(new ArrayList<>(requests.size()));

        final AwaitableChannelInitializer handler = handlerFactory.apply(latch, content);
        clientBootstrap.handler(handler);

        ChannelFuture channelFuture = null;
        try {
            channelFuture = clientBootstrap.connect(remoteAddress);
            channelFuture.sync();
            handler.await();

            for (HttpRequest request : requests) {
                channelFuture.channel().writeAndFlush(request);
            }
            if (latch.await(30L, TimeUnit.SECONDS) == false) {
                fail("Failed to get all expected responses.");
            }

        } finally {
            if (channelFuture != null) {
                channelFuture.channel().close().awaitUninterruptibly();
            }
        }

        return content;
    }

    @Override
    public void close() {
        clientBootstrap.config().group().shutdownGracefully().awaitUninterruptibly();
    }

    /**
     * helper factory which adds returned data to a list and uses a count down latch to decide when done
     */
    private static class CountDownLatchHandlerHttp extends AwaitableChannelInitializer {

        private final CountDownLatch latch;
        private final Collection<FullHttpResponse> content;

        CountDownLatchHandlerHttp(final CountDownLatch latch, final Collection<FullHttpResponse> content) {
            this.latch = latch;
            this.content = content;
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            final int maxContentLength = new ByteSizeValue(100, ByteSizeUnit.MB).bytesAsInt();
            ch.pipeline().addLast(new HttpResponseDecoder());
            ch.pipeline().addLast(new HttpRequestEncoder());
            ch.pipeline().addLast(new HttpContentDecompressor());
            ch.pipeline().addLast(new HttpObjectAggregator(maxContentLength));
            ch.pipeline().addLast(new SimpleChannelInboundHandler<HttpObject>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                    final FullHttpResponse response = (FullHttpResponse) msg;
                    // We copy the buffer manually to avoid a huge allocation on a pooled allocator. We have
                    // a test that tracks huge allocations, so we want to avoid them in this test code.
                    ByteBuf newContent = Unpooled.copiedBuffer(((FullHttpResponse) msg).content());
                    content.add(response.replace(newContent));
                    latch.countDown();
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                    super.exceptionCaught(ctx, cause);
                    latch.countDown();
                }
            });
        }

    }

    /**
     * The channel initializer with the ability to await for initialization to be completed
     *
     */
    private static abstract class AwaitableChannelInitializer extends ChannelInitializer<SocketChannel> {
        void await() {
            // do nothing
        }
    }

    /**
     * helper factory which adds returned data to a list and uses a count down latch to decide when done
     */
    private static class CountDownLatchHandlerHttp2 extends AwaitableChannelInitializer {

        private final CountDownLatch latch;
        private final Collection<FullHttpResponse> content;
        private Http2SettingsHandler settingsHandler;

        CountDownLatchHandlerHttp2(final CountDownLatch latch, final Collection<FullHttpResponse> content) {
            this.latch = latch;
            this.content = content;
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            final int maxContentLength = new ByteSizeValue(100, ByteSizeUnit.MB).bytesAsInt();
            final Http2Connection connection = new DefaultHttp2Connection(false);
            settingsHandler = new Http2SettingsHandler(ch.newPromise());

            final ChannelInboundHandler responseHandler = new SimpleChannelInboundHandler<HttpObject>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                    final FullHttpResponse response = (FullHttpResponse) msg;

                    // this is upgrade request, skipping it over
                    if (Boolean.TRUE.equals(ctx.channel().attr(AttributeKey.valueOf("upgrade")).getAndRemove())) {
                        return;
                    }

                    // We copy the buffer manually to avoid a huge allocation on a pooled allocator. We have
                    // a test that tracks huge allocations, so we want to avoid them in this test code.
                    ByteBuf newContent = Unpooled.copiedBuffer(((FullHttpResponse) msg).content());
                    content.add(response.replace(newContent));
                    latch.countDown();
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                    super.exceptionCaught(ctx, cause);
                    latch.countDown();
                }
            };

            final HttpToHttp2ConnectionHandler connectionHandler = new HttpToHttp2ConnectionHandlerBuilder().connection(connection)
                .frameListener(
                    new DelegatingDecompressorFrameListener(
                        connection,
                        new InboundHttp2ToHttpAdapterBuilder(connection).maxContentLength(maxContentLength).propagateSettings(true).build()
                    )
                )
                .build();

            final HttpClientCodec sourceCodec = new HttpClientCodec();
            final Http2ClientUpgradeCodec upgradeCodec = new Http2ClientUpgradeCodec(connectionHandler);
            final HttpClientUpgradeHandler upgradeHandler = new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, maxContentLength);

            ch.pipeline().addLast(sourceCodec);
            ch.pipeline().addLast(upgradeHandler);
            ch.pipeline().addLast(new HttpContentDecompressor());
            ch.pipeline().addLast(new UpgradeRequestHandler(settingsHandler, responseHandler));
        }

        @Override
        void await() {
            try {
                // Await for HTTP/2 settings being sent over before moving on to sending the requests
                settingsHandler.awaitSettings(5, TimeUnit.SECONDS);
            } catch (final Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * A handler that triggers the cleartext upgrade to HTTP/2 (h2c) by sending an
     * initial HTTP request.
     */
    private static class UpgradeRequestHandler extends ChannelInboundHandlerAdapter {
        private final ChannelInboundHandler settingsHandler;
        private final ChannelInboundHandler responseHandler;

        UpgradeRequestHandler(final ChannelInboundHandler settingsHandler, final ChannelInboundHandler responseHandler) {
            this.settingsHandler = settingsHandler;
            this.responseHandler = responseHandler;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            // The first request is HTTP/2 protocol upgrade (since we support only h2c there)
            final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            request.headers().add(HttpHeaderNames.HOST, "localhost");
            request.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), "http");

            ctx.channel().attr(AttributeKey.valueOf("upgrade")).set(true);
            ctx.writeAndFlush(request);
            ctx.fireChannelActive();

            ctx.pipeline().remove(this);
            ctx.pipeline().addLast(settingsHandler);
            ctx.pipeline().addLast(responseHandler);
        }
    }

    private static class Http2SettingsHandler extends SimpleChannelInboundHandler<Http2Settings> {
        private ChannelPromise promise;

        Http2SettingsHandler(ChannelPromise promise) {
            this.promise = promise;
        }

        /**
         * Wait for this handler to be added after the upgrade to HTTP/2, and for initial preface
         * handshake to complete.
         */
        void awaitSettings(long timeout, TimeUnit unit) throws Exception {
            if (!promise.awaitUninterruptibly(timeout, unit)) {
                throw new IllegalStateException("Timed out waiting for HTTP/2 settings");
            }
            if (!promise.isSuccess()) {
                throw new RuntimeException(promise.cause());
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Http2Settings msg) throws Exception {
            promise.setSuccess();
            ctx.pipeline().remove(this);
        }
    }

}
