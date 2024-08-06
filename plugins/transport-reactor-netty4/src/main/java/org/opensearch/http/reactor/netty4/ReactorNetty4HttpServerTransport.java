/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.common.Nullable;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.util.net.NetUtils;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.AbstractHttpServerTransport;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpReadTimeoutException;
import org.opensearch.http.HttpServerChannel;
import org.opensearch.http.reactor.netty4.ssl.SslUtils;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.reactor.SharedGroupFactory;
import org.opensearch.transport.reactor.netty4.Netty4Utils;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSessionContext;

import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_CONNECT_TIMEOUT;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_ALIVE;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_COUNT;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_IDLE;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_INTERVAL;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_TCP_REUSE_ADDRESS;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_TCP_SEND_BUFFER_SIZE;

/**
 * The HTTP transport implementations based on Reactor Netty (see please {@link HttpServer}).
 */
public class ReactorNetty4HttpServerTransport extends AbstractHttpServerTransport {
    private static final String SETTING_KEY_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS = "http.netty.max_composite_buffer_components";
    private static final ByteSizeValue MTU = new ByteSizeValue(Long.parseLong(System.getProperty("opensearch.net.mtu", "1500")));

    /**
     * The number of Reactor Netty HTTP workers
     */
    public static final Setting<Integer> SETTING_HTTP_WORKER_COUNT = Setting.intSetting("http.netty.worker_count", 0, Property.NodeScope);

    /**
     * The maximum number of composite components for request accumulation
     */
    public static Setting<Integer> SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS = new Setting<>(
        SETTING_KEY_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
        (s) -> {
            ByteSizeValue maxContentLength = SETTING_HTTP_MAX_CONTENT_LENGTH.get(s);
            /*
             * Netty accumulates buffers containing data from all incoming network packets that make up one HTTP request in an instance of
             * io.netty.buffer.CompositeByteBuf (think of it as a buffer of buffers). Once its capacity is reached, the buffer will iterate
             * over its individual entries and put them into larger buffers (see io.netty.buffer.CompositeByteBuf#consolidateIfNeeded()
             * for implementation details). We want to to resize that buffer because this leads to additional garbage on the heap and also
             * increases the application's native memory footprint (as direct byte buffers hold their contents off-heap).
             *
             * With this setting we control the CompositeByteBuf's capacity (which is by default 1024, see
             * io.netty.handler.codec.MessageAggregator#DEFAULT_MAX_COMPOSITEBUFFER_COMPONENTS). To determine a proper default capacity for
             * that buffer, we need to consider that the upper bound for the size of HTTP requests is determined by `maxContentLength`. The
             * number of buffers that are needed depend on how often Netty reads network packets which depends on the network type (MTU).
             * We assume here that OpenSearch receives HTTP requests via an Ethernet connection which has a MTU of 1500 bytes.
             *
             * Note that we are *not* pre-allocating any memory based on this setting but rather determine the CompositeByteBuf's capacity.
             * The tradeoff is between less (but larger) buffers that are contained in the CompositeByteBuf and more (but smaller) buffers.
             * With the default max content length of 100MB and a MTU of 1500 bytes we would allow 69905 entries.
             */
            long maxBufferComponentsEstimate = Math.round((double) (maxContentLength.getBytes() / MTU.getBytes()));
            // clamp value to the allowed range
            long maxBufferComponents = Math.max(2, Math.min(maxBufferComponentsEstimate, Integer.MAX_VALUE));
            return String.valueOf(maxBufferComponents);
            // Netty's CompositeByteBuf implementation does not allow less than two components.
        },
        s -> Setting.parseInt(s, 2, Integer.MAX_VALUE, SETTING_KEY_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS),
        Property.NodeScope
    );

    private final SharedGroupFactory sharedGroupFactory;
    private final int readTimeoutMillis;
    private final int connectTimeoutMillis;
    private final int maxCompositeBufferComponents;
    private final ByteSizeValue maxInitialLineLength;
    private final ByteSizeValue maxHeaderSize;
    private final ByteSizeValue maxChunkSize;
    private final SecureHttpTransportSettingsProvider secureHttpTransportSettingsProvider;
    private volatile SharedGroupFactory.SharedGroup sharedGroup;
    private volatile DisposableServer disposableServer;
    private volatile Scheduler scheduler;

    /**
     * Creates new HTTP transport implementations based on Reactor Netty (see please {@link HttpServer}).
     * @param settings settings
     * @param networkService network service
     * @param bigArrays big array allocator
     * @param threadPool thread pool instance
     * @param xContentRegistry XContent registry instance
     * @param dispatcher dispatcher instance
     * @param clusterSettings cluster settings
     * @param sharedGroupFactory shared group factory
     * @param tracer tracer instance
     */
    public ReactorNetty4HttpServerTransport(
        Settings settings,
        NetworkService networkService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        SharedGroupFactory sharedGroupFactory,
        Tracer tracer
    ) {
        this(
            settings,
            networkService,
            bigArrays,
            threadPool,
            xContentRegistry,
            dispatcher,
            clusterSettings,
            sharedGroupFactory,
            null,
            tracer
        );
    }

    /**
     * Creates new HTTP transport implementations based on Reactor Netty (see please {@link HttpServer}).
     * @param settings settings
     * @param networkService network service
     * @param bigArrays big array allocator
     * @param threadPool thread pool instance
     * @param xContentRegistry XContent registry instance
     * @param dispatcher dispatcher instance
     * @param clusterSettings cluster settings
     * @param sharedGroupFactory shared group factory
     * @param secureHttpTransportSettingsProvider secure HTTP transport settings provider
     * @param tracer tracer instance
     */
    public ReactorNetty4HttpServerTransport(
        Settings settings,
        NetworkService networkService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        SharedGroupFactory sharedGroupFactory,
        @Nullable SecureHttpTransportSettingsProvider secureHttpTransportSettingsProvider,
        Tracer tracer
    ) {
        super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher, clusterSettings, tracer);
        Netty4Utils.setAvailableProcessors(OpenSearchExecutors.NODE_PROCESSORS_SETTING.get(settings));
        this.readTimeoutMillis = Math.toIntExact(SETTING_HTTP_READ_TIMEOUT.get(settings).getMillis());
        this.connectTimeoutMillis = Math.toIntExact(SETTING_HTTP_CONNECT_TIMEOUT.get(settings).getMillis());
        this.sharedGroupFactory = sharedGroupFactory;
        this.maxCompositeBufferComponents = SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS.get(settings);
        this.maxChunkSize = SETTING_HTTP_MAX_CHUNK_SIZE.get(settings);
        this.maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        this.maxInitialLineLength = SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings);
        this.secureHttpTransportSettingsProvider = secureHttpTransportSettingsProvider;
    }

    /**
     * Binds the transport engine to the socket address
     * @param socketAddress socket address to bind to
     */
    @Override
    protected HttpServerChannel bind(InetSocketAddress socketAddress) throws Exception {
        final HttpServer server = configure(
            HttpServer.create()
                .httpFormDecoder(builder -> builder.scheduler(scheduler))
                .idleTimeout(Duration.ofMillis(connectTimeoutMillis))
                .readTimeout(Duration.ofMillis(readTimeoutMillis))
                .runOn(sharedGroup.getLowLevelGroup())
                .bindAddress(() -> socketAddress)
                .compress(true)
                .httpRequestDecoder(
                    spec -> spec.maxChunkSize(maxChunkSize.bytesAsInt())
                        .maxHeaderSize(maxHeaderSize.bytesAsInt())
                        .maxInitialLineLength(maxInitialLineLength.bytesAsInt())
                )
                .handle((req, res) -> incomingRequest(req, res))
        );

        disposableServer = server.bindNow();
        return new ReactorNetty4HttpServerChannel(disposableServer.channel());
    }

    private HttpServer configure(final HttpServer server) throws Exception {
        HttpServer configured = server.childOption(ChannelOption.TCP_NODELAY, SETTING_HTTP_TCP_NO_DELAY.get(settings))
            .childOption(ChannelOption.SO_KEEPALIVE, SETTING_HTTP_TCP_KEEP_ALIVE.get(settings));

        if (SETTING_HTTP_TCP_KEEP_ALIVE.get(settings)) {
            // Netty logs a warning if it can't set the option, so try this only on supported platforms
            if (IOUtils.LINUX || IOUtils.MAC_OS_X) {
                if (SETTING_HTTP_TCP_KEEP_IDLE.get(settings) >= 0) {
                    final SocketOption<Integer> keepIdleOption = NetUtils.getTcpKeepIdleSocketOptionOrNull();
                    if (keepIdleOption != null) {
                        configured = configured.childOption(NioChannelOption.of(keepIdleOption), SETTING_HTTP_TCP_KEEP_IDLE.get(settings));
                    }
                }
                if (SETTING_HTTP_TCP_KEEP_INTERVAL.get(settings) >= 0) {
                    final SocketOption<Integer> keepIntervalOption = NetUtils.getTcpKeepIntervalSocketOptionOrNull();
                    if (keepIntervalOption != null) {
                        configured = configured.childOption(
                            NioChannelOption.of(keepIntervalOption),
                            SETTING_HTTP_TCP_KEEP_INTERVAL.get(settings)
                        );
                    }
                }
                if (SETTING_HTTP_TCP_KEEP_COUNT.get(settings) >= 0) {
                    final SocketOption<Integer> keepCountOption = NetUtils.getTcpKeepCountSocketOptionOrNull();
                    if (keepCountOption != null) {
                        configured = configured.childOption(
                            NioChannelOption.of(keepCountOption),
                            SETTING_HTTP_TCP_KEEP_COUNT.get(settings)
                        );
                    }
                }
            }
        }

        final ByteSizeValue tcpSendBufferSize = SETTING_HTTP_TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.getBytes() > 0) {
            configured = configured.childOption(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.getBytes()));
        }

        final ByteSizeValue tcpReceiveBufferSize = SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.getBytes() > 0) {
            configured = configured.childOption(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.getBytes()));
        }

        final boolean reuseAddress = SETTING_HTTP_TCP_REUSE_ADDRESS.get(settings);
        configured = configured.option(ChannelOption.SO_REUSEADDR, reuseAddress);
        configured = configured.childOption(ChannelOption.SO_REUSEADDR, reuseAddress);

        // Configure SSL context if available
        if (secureHttpTransportSettingsProvider != null) {
            final SSLEngine engine = secureHttpTransportSettingsProvider.buildSecureHttpServerEngine(settings, this)
                .orElseGet(SslUtils::createDefaultServerSSLEngine);

            try {
                final List<String> cipherSuites = Arrays.asList(engine.getEnabledCipherSuites());
                final List<String> applicationProtocols = Arrays.asList(engine.getSSLParameters().getApplicationProtocols());

                configured = configured.secure(spec -> spec.sslContext(new SslContext() {
                    @Override
                    public SSLSessionContext sessionContext() {
                        throw new UnsupportedOperationException(); /* server only, should never be called */
                    }

                    @Override
                    public SSLEngine newEngine(ByteBufAllocator alloc, String peerHost, int peerPort) {
                        throw new UnsupportedOperationException(); /* server only, should never be called */
                    }

                    @Override
                    public SSLEngine newEngine(ByteBufAllocator alloc) {
                        try {
                            return secureHttpTransportSettingsProvider.buildSecureHttpServerEngine(
                                settings,
                                ReactorNetty4HttpServerTransport.this
                            ).orElseGet(SslUtils::createDefaultServerSSLEngine);
                        } catch (final SSLException ex) {
                            throw new UnsupportedOperationException("Unable to create SSLEngine", ex);
                        }
                    }

                    @Override
                    public boolean isClient() {
                        return false; /* server only */
                    }

                    @Override
                    public List<String> cipherSuites() {
                        return cipherSuites;
                    }

                    @Override
                    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
                        return new ApplicationProtocolNegotiator() {
                            @Override
                            public List<String> protocols() {
                                return applicationProtocols;
                            }
                        };
                    }
                }).build()).protocol(HttpProtocol.HTTP11, HttpProtocol.H2);
            } finally {
                ReferenceCountUtil.release(engine);
            }
        } else {
            configured = configured.protocol(HttpProtocol.HTTP11, HttpProtocol.H2C);
        }

        return configured;
    }

    /**
     * Handles incoming Reactor Netty request
     * @param request request instance
     * @param response response instances
     * @return response publisher
     */
    protected Publisher<Void> incomingRequest(HttpServerRequest request, HttpServerResponse response) {
        final Method method = HttpConversionUtil.convertMethod(request.method());
        final Optional<RestHandler> dispatchHandlerOpt = dispatcher.dispatchHandler(
            request.uri(),
            request.fullPath(),
            method,
            request.params()
        );
        if (dispatchHandlerOpt.map(RestHandler::supportsStreaming).orElse(false)) {
            final ReactorNetty4StreamingRequestConsumer<HttpContent> consumer = new ReactorNetty4StreamingRequestConsumer<>(
                request,
                response
            );

            request.receiveContent()
                .switchIfEmpty(Mono.just(DefaultLastHttpContent.EMPTY_LAST_CONTENT))
                .subscribe(consumer, error -> {}, () -> consumer.accept(DefaultLastHttpContent.EMPTY_LAST_CONTENT));

            incomingStream(new ReactorNetty4HttpRequest(request), consumer.httpChannel());
            return response.sendObject(consumer);
        } else {
            final ReactorNetty4NonStreamingRequestConsumer<HttpContent> consumer = new ReactorNetty4NonStreamingRequestConsumer<>(
                this,
                request,
                response,
                maxCompositeBufferComponents
            );

            request.receiveContent().switchIfEmpty(Mono.just(DefaultLastHttpContent.EMPTY_LAST_CONTENT)).subscribe(consumer);

            return Mono.from(consumer).flatMap(hc -> {
                final FullHttpResponse r = (FullHttpResponse) hc;
                response.status(r.status());
                response.trailerHeaders(c -> r.trailingHeaders().forEach(h -> c.add(h.getKey(), h.getValue())));
                response.chunkedTransfer(false);
                response.compression(true);
                r.headers().forEach(h -> response.addHeader(h.getKey(), h.getValue()));
                return Mono.from(response.sendObject(r.content()));
            });
        }
    }

    /**
     * Called to tear down internal resources
     */
    @Override
    protected void stopInternal() {
        if (sharedGroup != null) {
            sharedGroup.shutdown();
            sharedGroup = null;
        }

        if (scheduler != null) {
            scheduler.dispose();
            scheduler = null;
        }

        if (disposableServer != null) {
            disposableServer.disposeNow();
            disposableServer = null;
        }
    }

    /**
     * Starts the transport
     */
    @Override
    protected void doStart() {
        boolean success = false;
        try {
            scheduler = Schedulers.newBoundedElastic(
                Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
                Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
                "http-form-decoder"
            );
            sharedGroup = sharedGroupFactory.getHttpGroup();
            bindServer();
            success = true;
        } finally {
            if (success == false) {
                doStop(); // otherwise we leak threads since we never moved to started
            }
        }
    }

    /**
     * Exception handler
     * @param channel HTTP channel
     * @param cause exception occurred
     */
    @Override
    public void onException(HttpChannel channel, Exception cause) {
        if (cause instanceof ReadTimeoutException) {
            super.onException(channel, new HttpReadTimeoutException(readTimeoutMillis, cause));
        } else {
            super.onException(channel, cause);
        }
    }
}
