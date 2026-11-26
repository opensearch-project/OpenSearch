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

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpPipelinedRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.NullDispatcher;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCounted;

import static org.opensearch.http.TestDispatcherBuilder.dispatcherBuilderWithDefaults;
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

    @SuppressForbidden(reason = "A raw socket is needed to disconnect while HTTP responses remain queued")
    public void testDisconnectReleasesQueuedResponseBreakerReservations() throws Exception {
        final Settings settings = Settings.builder()
            .put("http.port", "0")
            .put("http.host", "127.0.0.1")
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final AtomicReference<RestChannel> heldChannel = new AtomicReference<>();
        final AtomicReference<Exception> dispatchFailure = new AtomicReference<>();
        final CountDownLatch queuedResponses = new CountDownLatch(2);
        final HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().withDispatchRequest(
            (request, channel, context) -> {
                if (request.rawPath().equals("/hold")) {
                    heldChannel.set(channel);
                    return;
                }
                try {
                    // Allocate through the actual DefaultRestChannel, which releases this output from its HTTP completion listener.
                    BytesStreamOutput output = channel.bytesOutput();
                    output.writeByte((byte) 1);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, output.bytes()));
                } catch (RuntimeException e) {
                    dispatchFailure.set(e);
                } finally {
                    queuedResponses.countDown();
                }
            }
        ).build();
        try (
            HierarchyCircuitBreakerService breakerService = new HierarchyCircuitBreakerService(settings, List.of(), clusterSettings);
            Netty4HttpServerTransport transport = new Netty4HttpServerTransport(
                settings,
                networkService,
                new BigArrays(null, breakerService, CircuitBreaker.REQUEST).withCircuitBreaking(),
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings),
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
            assertEquals(0, breaker.getUsed());
            try {
                try (Socket socket = new Socket()) {
                    socket.connect(randomFrom(transport.boundAddress().boundAddresses()).address(), 10000);
                    socket.getOutputStream()
                        .write(
                            ("GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n"
                                + "GET /queued/1 HTTP/1.1\r\nHost: localhost\r\n\r\n"
                                + "GET /queued/2 HTTP/1.1\r\nHost: localhost\r\n\r\n").getBytes(StandardCharsets.US_ASCII)
                        );
                    socket.getOutputStream().flush();
                    assertTrue(queuedResponses.await(10, TimeUnit.SECONDS));
                    assertNull(dispatchFailure.get());
                    assertNotNull(heldChannel.get());
                    assertTrue("Later responses must remain charged while response zero is outstanding", breaker.getUsed() > 0);
                }
                // Only the client has closed: the server and the first request remain outstanding during this assertion.
                assertBusy(() -> assertEquals("Peer disconnect must release queued response outputs", 0, breaker.getUsed()));
            } finally {
                if (heldChannel.get() != null) {
                    heldChannel.get()
                        .sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
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
                new SharedGroupFactory(settings),
                NoopTracer.INSTANCE
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
            super(transport, transport.handlingSettings, HttpResponseHeadersFactories.newDefault());
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
