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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.http.BindHttpException;
import org.opensearch.http.CorsHandler;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.http.NullDispatcher;
import org.opensearch.nio.NioSocketChannel;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.nio.NioGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.opensearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link NioHttpServerTransport} class.
 */
public class NioHttpServerTransportTests extends OpenSearchTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private MockPageCacheRecycler pageRecycler;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        pageRecycler = new MockPageCacheRecycler(Settings.EMPTY);
        bigArrays = new MockBigArrays(pageRecycler, new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        bigArrays = null;
    }

    /**
     * Test that {@link NioHttpServerTransport} supports the "Expect: 100-continue" HTTP header
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectContinueHeader() throws InterruptedException {
        final Settings settings = createSettings();
        final int contentLength = randomIntBetween(1, HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).bytesAsInt());
        runExpectHeaderTest(settings, HttpHeaderValues.CONTINUE.toString(), contentLength, HttpResponseStatus.CONTINUE);
    }

    /**
     * Test that {@link NioHttpServerTransport} responds to a
     * 100-continue expectation with too large a content-length
     * with a 413 status.
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectContinueHeaderContentLengthTooLong() throws InterruptedException {
        final String key = HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey();
        final int maxContentLength = randomIntBetween(1, 104857600);
        final Settings settings = createBuilderWithPort().put(key, maxContentLength + "b").build();
        final int contentLength = randomIntBetween(maxContentLength + 1, Integer.MAX_VALUE);
        runExpectHeaderTest(settings, HttpHeaderValues.CONTINUE.toString(), contentLength, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
    }

    /**
     * Test that {@link NioHttpServerTransport} responds to an unsupported expectation with a 417 status.
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectUnsupportedExpectation() throws InterruptedException {
        final Settings settings = createSettings();
        runExpectHeaderTest(settings, "chocolate=yummy", 0, HttpResponseStatus.EXPECTATION_FAILED);
    }

    private void runExpectHeaderTest(
        final Settings settings,
        final String expectation,
        final int contentLength,
        final HttpResponseStatus expectedStatus
    ) throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                channel.sendResponse(new BytesRestResponse(OK, BytesRestResponse.TEXT_CONTENT_TYPE, new BytesArray("done")));
            }

            @Override
            public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError();
            }
        };
        try (
            NioHttpServerTransport transport = new NioHttpServerTransport(
                settings,
                networkService,
                bigArrays,
                pageRecycler,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new NioGroupFactory(settings, logger),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            try (NioHttpClient client = NioHttpClient.http()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
                request.headers().set(HttpHeaderNames.EXPECT, expectation);
                HttpUtil.setContentLength(request, contentLength);

                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(response.status(), equalTo(expectedStatus));
                    if (expectedStatus.equals(HttpResponseStatus.CONTINUE)) {
                        final FullHttpRequest continuationRequest = new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            Unpooled.EMPTY_BUFFER
                        );
                        final FullHttpResponse continuationResponse = client.send(remoteAddress.address(), continuationRequest);
                        try {
                            assertThat(continuationResponse.status(), is(HttpResponseStatus.OK));
                            assertThat(
                                new String(ByteBufUtil.getBytes(continuationResponse.content()), StandardCharsets.UTF_8),
                                is("done")
                            );
                        } finally {
                            continuationResponse.release();
                        }
                    }
                } finally {
                    response.release();
                }
            }
        }
    }

    public void testBindUnavailableAddress() {
        final Settings initialSettings = createSettings();
        try (
            NioHttpServerTransport transport = new NioHttpServerTransport(
                initialSettings,
                networkService,
                bigArrays,
                pageRecycler,
                threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                new NioGroupFactory(Settings.EMPTY, logger),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            Settings settings = Settings.builder()
                .put("http.port", remoteAddress.getPort())
                .put("network.host", remoteAddress.getAddress())
                .build();
            try (
                NioHttpServerTransport otherTransport = new NioHttpServerTransport(
                    settings,
                    networkService,
                    bigArrays,
                    pageRecycler,
                    threadPool,
                    xContentRegistry(),
                    new NullDispatcher(),
                    new NioGroupFactory(Settings.EMPTY, logger),
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    NoopTracer.INSTANCE
                )
            ) {
                BindHttpException bindHttpException = expectThrows(BindHttpException.class, () -> otherTransport.start());
                assertEquals("Failed to bind to " + NetworkAddress.format(remoteAddress.address()), bindHttpException.getMessage());
            }
        }
    }

    public void testCorsRequest() throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
                throw new AssertionError();
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError();
            }

        };

        final Settings settings = createBuilderWithPort().put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "test-cors.org")
            .build();

        try (
            NioHttpServerTransport transport = new NioHttpServerTransport(
                settings,
                networkService,
                bigArrays,
                pageRecycler,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new NioGroupFactory(settings, logger),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            // Test pre-flight request
            try (NioHttpClient client = NioHttpClient.http()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/");
                request.headers().add(CorsHandler.ORIGIN, "test-cors.org");
                request.headers().add(CorsHandler.ACCESS_CONTROL_REQUEST_METHOD, "POST");

                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    assertThat(response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), equalTo("test-cors.org"));
                    assertThat(response.headers().get(CorsHandler.VARY), equalTo(CorsHandler.ORIGIN));
                    assertTrue(response.headers().contains(CorsHandler.DATE));
                } finally {
                    response.release();
                }
            }

            // Test short-circuited request
            try (NioHttpClient client = NioHttpClient.http()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
                request.headers().add(CorsHandler.ORIGIN, "google.com");

                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.FORBIDDEN));
                } finally {
                    response.release();
                }
            }
        }
    }

    public void testLargeCompressedResponse() throws InterruptedException {
        final String responseString = randomAlphaOfLength(4 * 1024 * 1024);
        final String url = "/thing";
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                if (url.equals(request.uri())) {
                    channel.sendResponse(new BytesRestResponse(OK, responseString));
                } else {
                    logger.error("--> Unexpected successful uri [{}]", request.uri());
                    throw new AssertionError();
                }
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError();
            }

        };

        try (
            NioHttpServerTransport transport = new NioHttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                pageRecycler,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new NioGroupFactory(Settings.EMPTY, logger),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (NioHttpClient client = NioHttpClient.http()) {
                DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
                request.headers().add(HttpHeaderNames.ACCEPT_ENCODING, randomFrom("deflate", "gzip"));
                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    byte[] bytes = new byte[response.content().readableBytes()];
                    response.content().readBytes(bytes);
                    assertThat(new String(bytes, StandardCharsets.UTF_8), equalTo(responseString));
                } finally {
                    response.release();
                }
            }
        }
    }

    public void testBadRequest() throws InterruptedException {
        final AtomicReference<Throwable> causeReference = new AtomicReference<>();
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
                throw new AssertionError();
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                causeReference.set(cause);
                try {
                    final OpenSearchException e = new OpenSearchException("you sent a bad request and you should feel bad");
                    channel.sendResponse(new BytesRestResponse(channel, BAD_REQUEST, e));
                } catch (final IOException e) {
                    throw new AssertionError(e);
                }
            }

        };

        final Settings settings;
        final int maxInitialLineLength;
        final Setting<ByteSizeValue> httpMaxInitialLineLengthSetting = HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
        if (randomBoolean()) {
            maxInitialLineLength = httpMaxInitialLineLengthSetting.getDefault(Settings.EMPTY).bytesAsInt();
            settings = createSettings();
        } else {
            maxInitialLineLength = randomIntBetween(1, 8192);
            settings = createBuilderWithPort().put(httpMaxInitialLineLengthSetting.getKey(), maxInitialLineLength + "b").build();
        }

        try (
            NioHttpServerTransport transport = new NioHttpServerTransport(
                settings,
                networkService,
                bigArrays,
                pageRecycler,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new NioGroupFactory(settings, logger),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (NioHttpClient client = NioHttpClient.http()) {
                final String url = "/" + new String(new byte[maxInitialLineLength], Charset.forName("UTF-8"));
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);

                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.BAD_REQUEST));
                    assertThat(
                        new String(response.content().array(), Charset.forName("UTF-8")),
                        containsString("you sent a bad request and you should feel bad")
                    );
                } finally {
                    response.release();
                }
            }
        }

        assertNotNull(causeReference.get());
        assertThat(causeReference.get(), instanceOf(TooLongFrameException.class));
    }

    public void testReadTimeout() throws Exception {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
                throw new AssertionError("Should not have received a dispatched request");
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError("Should not have received a dispatched request");
            }

        };

        Settings settings = createBuilderWithPort().put(
            HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT.getKey(),
            new TimeValue(randomIntBetween(100, 300))
        ).build();

        try (
            NioHttpServerTransport transport = new NioHttpServerTransport(
                settings,
                networkService,
                bigArrays,
                pageRecycler,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new NioGroupFactory(settings, logger),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (NioHttpClient client = NioHttpClient.http()) {
                NioSocketChannel channel = null;
                try {
                    CountDownLatch channelClosedLatch = new CountDownLatch(1);
                    channel = client.connect(remoteAddress.address());
                    channel.addCloseListener((r, t) -> channelClosedLatch.countDown());
                    assertTrue("Channel should be closed due to read timeout", channelClosedLatch.await(1, TimeUnit.MINUTES));
                } finally {
                    if (channel != null) {
                        channel.close();
                    }
                }
            }
        }
    }

    private Settings createSettings() {
        return createBuilderWithPort().build();
    }

    private Settings.Builder createBuilderWithPort() {
        return Settings.builder().put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange());
    }
}
