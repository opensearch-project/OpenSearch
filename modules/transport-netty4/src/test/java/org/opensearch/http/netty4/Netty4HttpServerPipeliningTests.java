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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCounted;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.http.HttpPipelinedRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.NullDispatcher;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.contains;

/**
 * This test just tests, if he pipelining works in general with out any connection the OpenSearch handler
 */
public class Netty4HttpServerPipeliningTests extends OpenSearchTestCase {
    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    public void testThatHttpPipeliningWorks() throws Exception {
        final Settings settings = Settings.builder().put("http.port", "0").build();
        try (HttpServerTransport httpServerTransport = new CustomNettyHttpServerTransport(settings)) {
            httpServerTransport.start();
            final TransportAddress transportAddress = randomFrom(httpServerTransport.boundAddress().boundAddresses());

            final int numberOfRequests = randomIntBetween(4, 16);
            final List<String> requests = new ArrayList<>(numberOfRequests);
            for (int i = 0; i < numberOfRequests; i++) {
                if (rarely()) {
                    requests.add("/slow/" + i);
                } else {
                    requests.add("/" + i);
                }
            }

            try (Netty4HttpClient nettyHttpClient = Netty4HttpClient.http()) {
                Collection<FullHttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests.toArray(new String[] {}));
                try {
                    Collection<String> responseBodies = Netty4HttpClient.returnHttpResponseBodies(responses);
                    assertThat(responseBodies, contains(requests.toArray()));
                } finally {
                    responses.forEach(ReferenceCounted::release);
                }
            }
        }
    }

    class CustomNettyHttpServerTransport extends Netty4HttpServerTransport {

        private final ExecutorService executorService = Executors.newCachedThreadPool();

        CustomNettyHttpServerTransport(final Settings settings) {
            super(
                settings,
                Netty4HttpServerPipeliningTests.this.networkService,
                Netty4HttpServerPipeliningTests.this.bigArrays,
                Netty4HttpServerPipeliningTests.this.threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                new SharedGroupFactory(settings)
            );
        }

        @Override
        public ChannelHandler configureServerChannelHandler() {
            return new CustomHttpChannelHandler(this, executorService);
        }

        @Override
        protected void doClose() {
            executorService.shutdown();
            super.doClose();
        }

    }

    private class CustomHttpChannelHandler extends Netty4HttpServerTransport.HttpChannelHandler {

        private final ExecutorService executorService;

        CustomHttpChannelHandler(Netty4HttpServerTransport transport, ExecutorService executorService) {
            super(transport, transport.handlingSettings);
            this.executorService = executorService;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
        }

        @Override
        public ChannelHandler getRequestHandler() {
            return new PossiblySlowUpstreamHandler(executorService);
        }
    }

    class PossiblySlowUpstreamHandler extends SimpleChannelInboundHandler<HttpPipelinedRequest> {

        private final ExecutorService executorService;

        PossiblySlowUpstreamHandler(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpPipelinedRequest msg) throws Exception {
            executorService.submit(new PossiblySlowRunnable(ctx, msg));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.info("Caught exception", cause);
            ctx.channel().close().sync();
        }

    }

    class PossiblySlowRunnable implements Runnable {

        private ChannelHandlerContext ctx;
        private HttpPipelinedRequest pipelinedRequest;

        PossiblySlowRunnable(ChannelHandlerContext ctx, HttpPipelinedRequest msg) {
            this.ctx = ctx;
            this.pipelinedRequest = msg;
        }

        @Override
        public void run() {
            try {
                final String uri = pipelinedRequest.uri();

                final ByteBuf buffer = Unpooled.copiedBuffer(uri, StandardCharsets.UTF_8);

                HttpResponse response = pipelinedRequest.createResponse(
                    RestStatus.OK,
                    new BytesArray(uri.getBytes(StandardCharsets.UTF_8))
                );
                response.addHeader("content-length", Integer.toString(buffer.readableBytes()));

                final boolean slow = uri.matches("/slow/\\d+");
                if (slow) {
                    try {
                        Thread.sleep(scaledRandomIntBetween(500, 1000));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    assert uri.matches("/\\d+");
                }

                final ChannelPromise promise = ctx.newPromise();
                ctx.writeAndFlush(response, promise);
            } finally {
                pipelinedRequest.release();
            }
        }

    }

}
