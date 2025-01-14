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

package org.opensearch.http.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.nio.BytesChannelContext;
import org.opensearch.nio.ChannelFactory;
import org.opensearch.nio.Config;
import org.opensearch.nio.EventHandler;
import org.opensearch.nio.FlushOperation;
import org.opensearch.nio.InboundChannelBuffer;
import org.opensearch.nio.NioChannelHandler;
import org.opensearch.nio.NioSelector;
import org.opensearch.nio.NioSelectorGroup;
import org.opensearch.nio.NioServerSocketChannel;
import org.opensearch.nio.NioSocketChannel;
import org.opensearch.nio.SocketChannelContext;
import org.opensearch.nio.WriteOperation;
import org.opensearch.tasks.Task;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import static org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.fail;

/**
 * Tiny helper to send http requests over nio.
 */
public class NioHttpClient implements Closeable {

    static Collection<String> returnOpaqueIds(Collection<FullHttpResponse> responses) {
        List<String> list = new ArrayList<>(responses.size());
        for (HttpResponse response : responses) {
            list.add(response.headers().get(Task.X_OPAQUE_ID));
        }
        return list;
    }

    private static final Logger logger = LogManager.getLogger(NioHttpClient.class);

    private final NioSelectorGroup nioGroup;
    private final boolean secure;

    private NioHttpClient(final boolean secure) {
        try {
            this.secure = secure;
            nioGroup = new NioSelectorGroup(
                daemonThreadFactory(Settings.EMPTY, "nio-http-client"),
                1,
                (s) -> new EventHandler(this::onException, s)
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static NioHttpClient http() {
        return new NioHttpClient(false);
    }

    public static NioHttpClient https() {
        return new NioHttpClient(true);
    }

    public Collection<FullHttpResponse> get(InetSocketAddress remoteAddress, String... uris) throws InterruptedException {
        Collection<HttpRequest> requests = new ArrayList<>(uris.length);
        for (int i = 0; i < uris.length; i++) {
            final HttpRequest httpRequest = new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, uris[i]);
            httpRequest.headers().add(HOST, "localhost");
            httpRequest.headers().add(Task.X_OPAQUE_ID, String.valueOf(i));
            requests.add(httpRequest);
        }
        return sendRequests(remoteAddress, requests);
    }

    public final FullHttpResponse send(InetSocketAddress remoteAddress, FullHttpRequest httpRequest) throws InterruptedException {
        Collection<FullHttpResponse> responses = sendRequests(remoteAddress, Collections.singleton(httpRequest));
        assert responses.size() == 1 : "expected 1 and only 1 http response";
        return responses.iterator().next();
    }

    public final NioSocketChannel connect(InetSocketAddress remoteAddress) {
        ChannelFactory<NioServerSocketChannel, NioSocketChannel> factory = new ClientChannelFactory(
            new CountDownLatch(0),
            new ArrayList<>(),
            secure
        );
        try {
            NioSocketChannel nioSocketChannel = nioGroup.openChannel(remoteAddress, factory);
            PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
            nioSocketChannel.addConnectListener(ActionListener.toBiConsumer(connectFuture));
            connectFuture.actionGet();
            return nioSocketChannel;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void onException(Exception e) {
        logger.error("Exception from http client", e);
    }

    private synchronized Collection<FullHttpResponse> sendRequests(InetSocketAddress remoteAddress, Collection<HttpRequest> requests)
        throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(requests.size());
        final Collection<FullHttpResponse> content = Collections.synchronizedList(new ArrayList<>(requests.size()));

        ChannelFactory<NioServerSocketChannel, NioSocketChannel> factory = new ClientChannelFactory(latch, content, secure);

        NioSocketChannel nioSocketChannel = null;
        try {
            nioSocketChannel = nioGroup.openChannel(remoteAddress, factory);
            PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
            nioSocketChannel.addConnectListener(ActionListener.toBiConsumer(connectFuture));
            connectFuture.actionGet();

            for (HttpRequest request : requests) {
                nioSocketChannel.getContext().sendMessage(request, (v, e) -> {});
            }
            if (latch.await(30L, TimeUnit.SECONDS) == false) {
                fail("Failed to get all expected responses.");
            }

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (nioSocketChannel != null) {
                nioSocketChannel.close();
            }
        }

        return content;
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(nioGroup::close);
    }

    private class ClientChannelFactory extends ChannelFactory<NioServerSocketChannel, NioSocketChannel> {

        private final CountDownLatch latch;
        private final Collection<FullHttpResponse> content;
        private final boolean secure;

        private ClientChannelFactory(CountDownLatch latch, Collection<FullHttpResponse> content, final boolean secure) {
            super(
                NetworkService.TCP_NO_DELAY.get(Settings.EMPTY),
                NetworkService.TCP_KEEP_ALIVE.get(Settings.EMPTY),
                NetworkService.TCP_KEEP_IDLE.get(Settings.EMPTY),
                NetworkService.TCP_KEEP_INTERVAL.get(Settings.EMPTY),
                NetworkService.TCP_KEEP_COUNT.get(Settings.EMPTY),
                NetworkService.TCP_REUSE_ADDRESS.get(Settings.EMPTY),
                Math.toIntExact(NetworkService.TCP_SEND_BUFFER_SIZE.get(Settings.EMPTY).getBytes()),
                Math.toIntExact(NetworkService.TCP_RECEIVE_BUFFER_SIZE.get(Settings.EMPTY).getBytes())
            );
            this.latch = latch;
            this.content = content;
            this.secure = secure;
        }

        @Override
        public NioSocketChannel createChannel(NioSelector selector, java.nio.channels.SocketChannel channel, Config.Socket socketConfig)
            throws IOException {
            NioSocketChannel nioSocketChannel = new NioSocketChannel(channel);
            HttpClientHandler handler = new HttpClientHandler(nioSocketChannel, latch, content, secure);
            Consumer<Exception> exceptionHandler = (e) -> {
                latch.countDown();
                onException(e);
                nioSocketChannel.close();
            };
            SocketChannelContext context = new BytesChannelContext(
                nioSocketChannel,
                selector,
                socketConfig,
                exceptionHandler,
                handler,
                InboundChannelBuffer.allocatingInstance()
            );
            nioSocketChannel.setContext(context);
            return nioSocketChannel;
        }

        @Override
        public NioServerSocketChannel createServerChannel(
            NioSelector selector,
            ServerSocketChannel channel,
            Config.ServerSocket socketConfig
        ) {
            throw new UnsupportedOperationException("Cannot create server channel");
        }
    }

    private static class HttpClientHandler implements NioChannelHandler {

        private final NettyAdaptor adaptor;
        private final CountDownLatch latch;
        private final Collection<FullHttpResponse> content;

        private HttpClientHandler(
            NioSocketChannel channel,
            CountDownLatch latch,
            Collection<FullHttpResponse> content,
            final boolean secure
        ) throws IOException {
            this.latch = latch;
            this.content = content;
            final int maxContentLength = Math.toIntExact(new ByteSizeValue(100, ByteSizeUnit.MB).getBytes());
            List<ChannelHandler> handlers = new ArrayList<>(5);

            SslHandler sslHandler = null;
            if (secure) {
                sslHandler = new SslHandler(
                    SslContextBuilder.forClient()
                        .clientAuth(ClientAuth.NONE)
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build()
                        .newEngine(UnpooledByteBufAllocator.DEFAULT)
                );
            }

            handlers.add(new HttpResponseDecoder());
            handlers.add(new HttpRequestEncoder());
            handlers.add(new HttpContentDecompressor());
            handlers.add(new HttpObjectAggregator(maxContentLength));

            adaptor = new NettyAdaptor(sslHandler, handlers.toArray(new ChannelHandler[0]));
            adaptor.addCloseListener((v, e) -> channel.close());
        }

        @Override
        public void channelActive() {}

        @Override
        public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
            assert message instanceof HttpRequest : "Expected type HttpRequest.class, found: " + message.getClass();
            return new WriteOperation() {
                @Override
                public BiConsumer<Void, Exception> getListener() {
                    return listener;
                }

                @Override
                public SocketChannelContext getChannel() {
                    return context;
                }

                @Override
                public Object getObject() {
                    return message;
                }
            };
        }

        @Override
        public List<FlushOperation> writeToBytes(WriteOperation writeOperation) {
            adaptor.write(writeOperation);
            return pollFlushOperations();
        }

        @Override
        public List<FlushOperation> pollFlushOperations() {
            ArrayList<FlushOperation> copiedOperations = new ArrayList<>(adaptor.getOutboundCount());
            FlushOperation flushOperation;
            while ((flushOperation = adaptor.pollOutboundOperation()) != null) {
                copiedOperations.add(flushOperation);
            }
            return copiedOperations;
        }

        @Override
        public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
            int bytesConsumed = adaptor.read(channelBuffer.sliceAndRetainPagesTo(channelBuffer.getIndex()));
            Object message;
            while ((message = adaptor.pollInboundMessage()) != null) {
                handleResponse(message);
            }

            return bytesConsumed;
        }

        @Override
        public boolean closeNow() {
            return false;
        }

        @Override
        public void close() throws IOException {
            try {
                adaptor.close();
                // After closing the pipeline, we must poll to see if any new messages are available. This
                // is because HTTP supports a channel being closed as an end of content marker.
                Object message;
                while ((message = adaptor.pollInboundMessage()) != null) {
                    handleResponse(message);
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        private void handleResponse(Object message) {
            final FullHttpResponse response = (FullHttpResponse) message;
            DefaultFullHttpResponse newResponse = new DefaultFullHttpResponse(
                response.protocolVersion(),
                response.status(),
                Unpooled.copiedBuffer(response.content()),
                response.headers().copy(),
                response.trailingHeaders().copy()
            );
            response.release();
            content.add(newResponse);
            latch.countDown();
        }
    }

}
