/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4.ssl;

import org.opensearch.OpenSearchException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.fips.FipsAwareSslProvider;
import org.opensearch.http.BindHttpException;
import org.opensearch.http.CorsHandler;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.http.NullDispatcher;
import org.opensearch.http.netty4.Netty4HttpClient;
import org.opensearch.http.netty4.Netty4HttpRawClient;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider;
import org.opensearch.plugins.TransportExceptionHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NettyAllocator;
import org.opensearch.transport.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DuplexChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.opensearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.opensearch.http.TestDispatcherBuilder.dispatcherBuilderWithDefaults;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link SecureNetty4HttpServerTransport} class.
 */
public class SecureNetty4HttpServerTransportTests extends OpenSearchTestCase {

    private static final char[] PASSWORD = "password".toCharArray();

    // Cached contexts to avoid repeated keystore loading and SSL context creation
    // (especially slow in FIPS mode with BCFKS).
    private static volatile SslContext cachedServerSslContext;

    private static final FipsAwareSslProvider<SslContext> serverSslContextProvider = (
        String keyStoreType,
        String fileExtension,
        String jcaProvider,
        String jsseProvider) -> {
        if (cachedServerSslContext == null) {
            synchronized (SecureNetty4HttpServerTransportTests.class) {
                if (cachedServerSslContext == null) {
                    try {
                        final KeyStore keyStore = KeyStore.getInstance(keyStoreType, jcaProvider);

                        try (
                            var in = SecureNetty4HttpServerTransportTests.class.getResourceAsStream(
                                "/netty4-server-keystore" + fileExtension
                            )
                        ) {
                            keyStore.load(in, PASSWORD);
                        }

                        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
                            KeyManagerFactory.getDefaultAlgorithm(),
                            jsseProvider
                        );
                        keyManagerFactory.init(keyStore, PASSWORD);

                        cachedServerSslContext = SslContextBuilder.forServer(keyManagerFactory)
                            .trustManager(InsecureTrustManagerFactory.INSTANCE)
                            .build();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        return cachedServerSslContext;
    };

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private ClusterSettings clusterSettings;
    private SecureHttpTransportSettingsProvider secureHttpTransportSettingsProvider;

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        secureHttpTransportSettingsProvider = new SecureHttpTransportSettingsProvider() {
            @Override
            public Optional<TransportExceptionHandler> buildHttpServerExceptionHandler(Settings settings, HttpServerTransport transport) {
                return Optional.empty();
            }

            @Override
            public Optional<SSLEngine> buildSecureHttpServerEngine(Settings settings, HttpServerTransport transport) throws SSLException {
                return Optional.of(serverSslContextProvider.create().newEngine(NettyAllocator.getAllocator()));
            }
        };
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

    /**
     * Test that {@link SecureNetty4HttpServerTransport} supports the "Expect: 100-continue" HTTP header
     *
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectContinueHeader() throws InterruptedException {
        final Settings settings = createSettings();
        final int contentLength = randomIntBetween(1, HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).bytesAsInt());
        runExpectHeaderTest(settings, HttpHeaderValues.CONTINUE.toString(), contentLength, HttpResponseStatus.CONTINUE);
    }

    /**
     * Test that {@link SecureNetty4HttpServerTransport} responds to a
     * 100-continue expectation with too large a content-length
     * with a 413 status.
     *
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
     * Test that {@link SecureNetty4HttpServerTransport} responds to an unsupported expectation with a 417 status.
     *
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectUnsupportedExpectation() throws InterruptedException {
        Settings settings = createSettings();
        runExpectHeaderTest(settings, "chocolate=yummy", 0, HttpResponseStatus.EXPECTATION_FAILED);
    }

    public void testTerminalRejectionClosesTls12ThenTcpAndDrainsCiphertext() throws Exception {
        assertTerminalRejectionClosesTlsThenTcpAndDrainsCiphertext("TLSv1.2");
    }

    public void testTerminalRejectionClosesTls13ThenTcpAndDrainsCiphertext() throws Exception {
        assertTerminalRejectionClosesTlsThenTcpAndDrainsCiphertext("TLSv1.3");
    }

    @SuppressForbidden(reason = "A raw socket distinguishes TLS close_notify from TCP EOF and sends ciphertext during draining")
    private void assertTerminalRejectionClosesTlsThenTcpAndDrainsCiphertext(String protocol) throws Exception {
        final int limit = 1024;
        final Settings settings = createBuilderWithPort().put("http.host", "127.0.0.1")
            .put(HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), limit + "b")
            .build();
        final AtomicInteger dispatched = new AtomicInteger();
        final AtomicBoolean observingDrain = new AtomicBoolean();
        final AtomicInteger ciphertextForwardedToTls = new AtomicInteger();
        final CompletableFuture<Channel> accepted = new CompletableFuture<>();
        final HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().withDispatchRequest(
            (request, channel, context) -> dispatched.incrementAndGet()
        ).build();
        final FipsAwareSslProvider<SSLContext> clientContextProvider = (keyStoreType, extension, jcaProvider, jsseProvider) -> {
            try {
                final SSLContext context = SSLContext.getInstance("TLS", jsseProvider);
                context.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
                return context;
            } catch (GeneralSecurityException e) {
                throw new AssertionError("Cannot create the TLS client context", e);
            }
        };
        try (
            SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            ) {
                @Override
                public ChannelHandler configureServerChannelHandler() {
                    return new SslHttpChannelHandler(this, handlingSettings) {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            super.initChannel(channel);
                            channel.pipeline().addBefore("ssl_http", "tls_input_observer", new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    if (observingDrain.get()) {
                                        ciphertextForwardedToTls.addAndGet(((ByteBuf) msg).readableBytes());
                                    }
                                    ctx.fireChannelRead(msg);
                                }
                            });
                            accepted.complete(channel);
                        }
                    };
                }
            };
            Socket socket = new Socket()
        ) {
            transport.start();
            socket.connect(Netty4HttpRawClient.ipv4Address(transport.boundAddress().boundAddresses()), 10_000);
            socket.setSoTimeout(10_000);
            final SSLEngine engine = clientContextProvider.create().createSSLEngine();
            engine.setUseClientMode(true);
            engine.setEnabledProtocols(new String[] { protocol });
            final Netty4HttpRawTlsClient client = new Netty4HttpRawTlsClient(engine, socket);
            client.handshake();
            final byte[] request = new Netty4HttpRawClient.RequestBuilder("POST", "/").rawBody(new byte[2 * limit]).chunked(true).bytes();
            client.writeEncrypted(client.encrypt(request));
            // Encrypt the remainder before receiving close_notify; a TLS engine may stop wrapping once the peer closes.
            final byte[] trailingCiphertext = client.encrypt(new byte[4 * limit]);
            final String response = new String(client.readUntilCloseNotify(), StandardCharsets.US_ASCII).toLowerCase(Locale.ROOT);
            assertTrue(response.startsWith("http/1.1 413 "));
            assertThat(response, containsString("\r\nconnection: close\r\n"));
            assertThat(response, containsString("\r\ncontent-length: 0\r\n"));
            assertTrue(response.endsWith("\r\n\r\n"));
            assertEquals("TCP FIN must follow the authenticated TLS close_notify", -1, socket.getInputStream().read());

            final Channel serverChannel = accepted.get(10, TimeUnit.SECONDS);
            serverChannel.eventLoop().submit(() -> {
                assertTrue(serverChannel.isActive());
                assertTrue(((DuplexChannel) serverChannel).isOutputShutdown());
                assertNotNull(serverChannel.pipeline().get("http_rejection"));
                observingDrain.set(true);
            }).get(10, TimeUnit.SECONDS);
            client.writeEncrypted(trailingCiphertext);
            socket.shutdownOutput();
            assertTrue(serverChannel.closeFuture().await(10, TimeUnit.SECONDS));
            assertEquals("Remaining ciphertext must be discarded before TLS decryption", 0, ciphertextForwardedToTls.get());
            assertEquals(0, dispatched.get());
        }
    }

    private void runExpectHeaderTest(
        final Settings settings,
        final String expectation,
        final int contentLength,
        final HttpResponseStatus expectedStatus
    ) throws InterruptedException {
        HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().withDispatchRequest(
            (request, channel, threadContext) -> channel.sendResponse(
                new BytesRestResponse(OK, BytesRestResponse.TEXT_CONTENT_TYPE, new BytesArray("done"))
            )
        ).build();
        try (
            SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            try (Netty4HttpClient client = Netty4HttpClient.https()) {
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
        Settings initialSettings = createSettings();
        try (
            SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                initialSettings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                secureHttpTransportSettingsProvider,
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
                SecureNetty4HttpServerTransport otherTransport = new SecureNetty4HttpServerTransport(
                    settings,
                    networkService,
                    bigArrays,
                    threadPool,
                    xContentRegistry(),
                    new NullDispatcher(),
                    clusterSettings,
                    new SharedGroupFactory(settings),
                    secureHttpTransportSettingsProvider,
                    NoopTracer.INSTANCE
                )
            ) {
                BindHttpException bindHttpException = expectThrows(BindHttpException.class, otherTransport::start);
                assertEquals("Failed to bind to " + NetworkAddress.format(remoteAddress.address()), bindHttpException.getMessage());
            }
        }
    }

    public void testBadRequest() throws InterruptedException {
        final AtomicReference<Throwable> causeReference = new AtomicReference<>();
        final HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().withDispatchBadRequest(
            (channel, threadContext, cause) -> {
                causeReference.set(cause);
                try {
                    final OpenSearchException e = new OpenSearchException("you sent a bad request and you should feel bad");
                    channel.sendResponse(new BytesRestResponse(channel, BAD_REQUEST, e));
                } catch (final IOException e) {
                    throw new AssertionError(e);
                }
            }
        ).build();
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
            SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (Netty4HttpClient client = Netty4HttpClient.https()) {
                final String url = "/" + new String("_").repeat(maxInitialLineLength);
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

    public void testLargeCompressedResponse() throws InterruptedException {
        final Settings settings = createSettings();
        final String responseString = randomAlphaOfLength(4 * 1024 * 1024);
        final String url = "/thing";
        final HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().withDispatchRequest(
            (request, channel, threadContext) -> {
                if (url.equals(request.uri())) {
                    channel.sendResponse(new BytesRestResponse(OK, responseString));
                } else {
                    logger.error("--> Unexpected successful uri [{}]", request.uri());
                    throw new AssertionError();
                }
            }
        ).build();
        try (
            SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (Netty4HttpClient client = Netty4HttpClient.https()) {
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

    public void testCorsRequest() throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().build();
        final Settings settings = createBuilderWithPort().put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "test-cors.org")
            .build();

        try (
            SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            // Test pre-flight request
            try (Netty4HttpClient client = Netty4HttpClient.https()) {
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
            try (Netty4HttpClient client = Netty4HttpClient.https()) {
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

    public void testReadTimeout() throws Exception {
        HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().build();
        Settings settings = createBuilderWithPort().put(
            HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT.getKey(),
            new TimeValue(randomIntBetween(100, 300))
        ).build();

        NioEventLoopGroup group = new NioEventLoopGroup();
        try (
            SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            CountDownLatch channelClosedLatch = new CountDownLatch(1);

            Bootstrap clientBootstrap = new Bootstrap().option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ChannelHandlerAdapter() {
                        });

                    }
                })
                .group(group);
            ChannelFuture connect = clientBootstrap.connect(remoteAddress.address());
            connect.channel().closeFuture().addListener(future -> channelClosedLatch.countDown());

            assertTrue("Channel should be closed due to read timeout", channelClosedLatch.await(1, TimeUnit.MINUTES));

        } finally {
            group.shutdownGracefully().await();
        }
    }

    private Settings createSettings() {
        return createBuilderWithPort().build();
    }

    private Settings.Builder createBuilderWithPort() {
        return Settings.builder().put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange());
    }
}
