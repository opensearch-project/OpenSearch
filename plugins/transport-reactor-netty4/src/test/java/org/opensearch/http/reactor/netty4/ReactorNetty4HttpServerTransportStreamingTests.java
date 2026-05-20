/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.reactor.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link ReactorNetty4HttpServerTransport} class with streaming support.
 */
public class ReactorNetty4HttpServerTransportStreamingTests extends AbstractReactorNetty4HttpServerTransportStreamingTests {
    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private ClusterSettings clusterSettings;

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    @After
    public void shutdown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        bigArrays = null;
        clusterSettings = null;
    }

    public void testRequestResponseStreaming() throws InterruptedException {
        final String responseString = randomAlphaOfLength(4 * 1024);
        final String url = "/stream/";

        final ToXContent[] chunks = newChunks(responseString);
        final HttpServerTransport.Dispatcher dispatcher = createStreamingDispatcher(threadPool, url, responseString);

        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (ReactorHttpClient client = ReactorHttpClient.create(false, Settings.EMPTY)) {
                HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
                final FullHttpResponse response = client.stream(remoteAddress.address(), request, Arrays.stream(chunks));
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    byte[] bytes = new byte[response.content().readableBytes()];
                    response.content().readBytes(bytes);
                    assertThat(new String(bytes, StandardCharsets.UTF_8), equalTo(Arrays.stream(newChunks(responseString)).map(s -> {
                        try (XContentBuilder builder = XContentType.JSON.contentBuilder()) {
                            return s.toXContent(builder, ToXContent.EMPTY_PARAMS).toString();
                        } catch (final IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    }).collect(Collectors.joining("\r\n", "", "\r\n"))));
                } finally {
                    response.release();
                }
            }
        }
    }

    public void testConnectionsGettingClosedForStreamingRequests() throws InterruptedException {
        final String responseString = randomAlphaOfLength(4 * 1024);
        final String url = "/stream/";

        final ToXContent[] chunks = newChunks(responseString);
        final HttpServerTransport.Dispatcher dispatcher = createStreamingDispatcher(threadPool, url, responseString);

        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                NoopTracer.INSTANCE
            );
            ReactorHttpClient client = ReactorHttpClient.create(false, Settings.EMPTY)
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
            long numRequests = randomLongBetween(5L, 15L);
            for (int i = 0; i < numRequests; i++) {
                logger.info("Sending request {}/{}", i + 1, numRequests);
                final FullHttpResponse response = client.stream(remoteAddress.address(), request, Arrays.stream(chunks));
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                } finally {
                    response.release();
                }
            }
            assertThat(transport.stats().getServerOpen(), equalTo(0L));
            assertThat(transport.stats().getTotalOpen(), equalTo(numRequests));
        }
    }
}
