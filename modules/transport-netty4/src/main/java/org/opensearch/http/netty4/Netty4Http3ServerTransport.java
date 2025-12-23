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
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.AbstractHttpServerTransport;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpHandlingSettings;
import org.opensearch.http.HttpReadTimeoutException;
import org.opensearch.http.HttpServerChannel;
import org.opensearch.http.netty4.http3.SecureQuicTokenHandler;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider.SecureHttpTransportParameters;
import org.opensearch.plugins.TransportExceptionHandler;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NettyAllocator;
import org.opensearch.transport.NettyByteBufSizer;
import org.opensearch.transport.SharedGroupFactory;
import org.opensearch.transport.netty4.Netty4Utils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.CompressionOptions;
import io.netty.handler.codec.compression.DeflateOptions;
import io.netty.handler.codec.compression.GzipOptions;
import io.netty.handler.codec.compression.StandardCompressionOptions;
import io.netty.handler.codec.compression.ZstdEncoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3FrameToHttpObjectCodec;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicSslEngine;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;

import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_CONNECT_TIMEOUT;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT;
import static org.opensearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;
import static org.opensearch.http.netty4.Netty4HttpServerTransport.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS;
import static org.opensearch.http.netty4.Netty4HttpServerTransport.SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE;

/**
 * The HTTP/3 transport implementations based on Netty 4.
 */
public class Netty4Http3ServerTransport extends AbstractHttpServerTransport {
    private static final Logger logger = LogManager.getLogger(Netty4Http3ServerTransport.class);

    /**
     * Set the initial maximum data limit for local bidirectional streams (in bytes).
     */
    public static final Setting<ByteSizeValue> SETTING_H3_MAX_STREAM_LOCAL_LENGTH = Setting.byteSizeSetting(
        "h3.max_stream_local_length",
        new ByteSizeValue(1000000, ByteSizeUnit.BYTES),
        Property.NodeScope
    );

    /**
     * Set the initial maximum data limit for remote bidirectional streams (in bytes).
     */
    public static final Setting<ByteSizeValue> SETTING_H3_MAX_STREAM_REMOTE_LENGTH = Setting.byteSizeSetting(
        "h3.max_stream_remote_length",
        new ByteSizeValue(1000000, ByteSizeUnit.BYTES),
        Property.NodeScope
    );

    /**
     * Set the initial maximum stream limit for bidirectional streams.
     *
     * The HTTP/3 standard expects that each end configures at least 100
     * concurrent bidirectional streams at a time, to avoid reducing performance
     * by reducing parallelism.
     */
    public static final Setting<Long> SETTING_H3_MAX_STREAMS = Setting.longSetting("h3.max_streams", 100L, Property.NodeScope);

    private final ByteSizeValue maxInitialLineLength;
    private final ByteSizeValue maxHeaderSize;
    private final ByteSizeValue maxChunkSize;

    private final SharedGroupFactory sharedGroupFactory;
    private final RecvByteBufAllocator recvByteBufAllocator;
    private final int readTimeoutMillis;
    private final int connectTimeoutMillis;

    private final int maxCompositeBufferComponents;
    private final int pipeliningMaxEvents;

    private volatile Bootstrap bootstrap;
    private volatile SharedGroupFactory.SharedGroup sharedGroup;
    private final SecureHttpTransportSettingsProvider secureHttpTransportSettingsProvider;
    private final TransportExceptionHandler exceptionHandler;

    /**
     * Creates new HTTP transport implementations based on Netty 4
     * @param settings settings
     * @param networkService network service
     * @param bigArrays big array allocator
     * @param threadPool thread pool instance
     * @param xContentRegistry XContent registry instance
     * @param dispatcher dispatcher instance
     * @param clusterSettings cluster settings
     * @param sharedGroupFactory shared group factory
     */
    public Netty4Http3ServerTransport(
        final Settings settings,
        final NetworkService networkService,
        final BigArrays bigArrays,
        final ThreadPool threadPool,
        final NamedXContentRegistry xContentRegistry,
        final Dispatcher dispatcher,
        final ClusterSettings clusterSettings,
        final SharedGroupFactory sharedGroupFactory,
        final SecureHttpTransportSettingsProvider secureHttpTransportSettingsProvider,
        final Tracer tracer
    ) {
        super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher, clusterSettings, tracer);
        Netty4Utils.setAvailableProcessors(OpenSearchExecutors.NODE_PROCESSORS_SETTING.get(settings));
        NettyAllocator.logAllocatorDescriptionIfNeeded();
        this.sharedGroupFactory = sharedGroupFactory;
        this.secureHttpTransportSettingsProvider = secureHttpTransportSettingsProvider;
        this.exceptionHandler = secureHttpTransportSettingsProvider.buildHttpServerExceptionHandler(settings, this)
            .orElse(TransportExceptionHandler.NOOP);

        this.maxChunkSize = SETTING_HTTP_MAX_CHUNK_SIZE.get(settings);
        this.maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        this.maxInitialLineLength = SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings);
        this.pipeliningMaxEvents = SETTING_PIPELINING_MAX_EVENTS.get(settings);

        this.maxCompositeBufferComponents = SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS.get(settings);

        this.readTimeoutMillis = Math.toIntExact(SETTING_HTTP_READ_TIMEOUT.get(settings).getMillis());
        this.connectTimeoutMillis = Math.toIntExact(SETTING_HTTP_CONNECT_TIMEOUT.get(settings).getMillis());

        ByteSizeValue receivePredictor = SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE.get(settings);
        recvByteBufAllocator = new FixedRecvByteBufAllocator(receivePredictor.bytesAsInt());

        logger.debug(
            "using max_chunk_size[{}], max_header_size[{}], max_initial_line_length[{}], max_content_length[{}], "
                + "receive_predictor[{}], max_composite_buffer_components[{}]",
            maxChunkSize,
            maxHeaderSize,
            maxInitialLineLength,
            maxContentLength,
            receivePredictor,
            maxCompositeBufferComponents
        );
    }

    public Settings settings() {
        return this.settings;
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            sharedGroup = sharedGroupFactory.getHttpGroup();
            bootstrap = new Bootstrap();

            bootstrap.group(sharedGroup.getLowLevelGroup());

            // NettyAllocator will return the channel type designed to work with the configuredAllocator
            bootstrap.channel(NioDatagramChannel.class);

            // Set the allocators for both the server channel and the child channels created
            bootstrap.option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator(true));
            bootstrap.handler(configureServerChannelHandler());
            bootstrap.option(ChannelOption.RECVBUF_ALLOCATOR, recvByteBufAllocator);

            bindServer();
            success = true;
        } finally {
            if (success == false) {
                doStop(); // otherwise we leak threads since we never moved to started
            }
        }
    }

    @Override
    protected HttpServerChannel bind(InetSocketAddress socketAddress) throws Exception {
        ChannelFuture future = bootstrap.bind(socketAddress).sync();
        Channel channel = future.channel();
        Netty4HttpServerChannel httpServerChannel = new Netty4HttpServerChannel(channel);
        channel.attr(HTTP_SERVER_CHANNEL_KEY).set(httpServerChannel);
        return httpServerChannel;
    }

    @Override
    protected void stopInternal() {
        if (sharedGroup != null) {
            sharedGroup.shutdown();
            sharedGroup = null;
        }
    }

    @Override
    public void onException(HttpChannel channel, Exception cause0) {
        Throwable cause = cause0;

        if (cause0 instanceof DecoderException) {
            cause = cause0.getCause();
        }

        exceptionHandler.onError(cause);
        logger.error("Exception during establishing a HTTP/3 connection: " + cause, cause);

        if (cause instanceof ReadTimeoutException) {
            super.onException(channel, new HttpReadTimeoutException(readTimeoutMillis, cause0));
        } else {
            super.onException(channel, cause0);
        }
    }

    public ChannelHandler configureServerChannelHandler() {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                final Optional<SecureHttpTransportParameters> parameters = secureHttpTransportSettingsProvider.parameters(settings);

                final KeyManagerFactory keyManagerFactory = parameters.flatMap(SecureHttpTransportParameters::keyManagerFactory)
                    .orElseThrow(() -> new OpenSearchException("The KeyManagerFactory instance is not provided"));

                final QuicSslContextBuilder sslContextBuilder = QuicSslContextBuilder.forServer(keyManagerFactory, null);

                parameters.flatMap(SecureHttpTransportParameters::trustManagerFactory).ifPresent(sslContextBuilder::trustManager);
                parameters.flatMap(SecureHttpTransportParameters::clientAuth)
                    .ifPresent(clientAuth -> sslContextBuilder.clientAuth(ClientAuth.valueOf(clientAuth)));

                final QuicSslContext sslContext = sslContextBuilder.applicationProtocols(
                    io.netty.handler.codec.http3.Http3.supportedApplicationProtocols()
                ).build();

                ch.pipeline().addLast(Http3.newQuicServerCodecBuilder().sslEngineProvider(q -> {
                    final QuicSslEngine engine = sslContext.newEngine(q.alloc());
                    q.attr(HTTP_SERVER_ENGINE_KEY).set(engine);
                    return engine;
                })
                    .maxIdleTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
                    .initialMaxData(SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).getBytes())
                    .initialMaxStreamDataBidirectionalLocal(SETTING_H3_MAX_STREAM_LOCAL_LENGTH.get(settings).getBytes())
                    .initialMaxStreamDataBidirectionalRemote(SETTING_H3_MAX_STREAM_REMOTE_LENGTH.get(settings).getBytes())
                    .initialMaxStreamsBidirectional(SETTING_H3_MAX_STREAMS.get(settings).longValue())
                    .tokenHandler(new SecureQuicTokenHandler())
                    .handler(new ChannelInitializer<QuicChannel>() {
                        @Override
                        protected void initChannel(QuicChannel ch) {
                            // Called for each connection
                            ch.pipeline()
                                .addLast(
                                    new Http3ServerConnectionHandler(
                                        new HttpChannelHandler(Netty4Http3ServerTransport.this, handlingSettings)
                                    )
                                );
                        }
                    })
                    .build());
            }
        };
    }

    public static final AttributeKey<Netty4HttpChannel> HTTP_CHANNEL_KEY = AttributeKey.newInstance("opensearch-quic-channel");
    protected static final AttributeKey<SSLEngine> HTTP_SERVER_ENGINE_KEY = AttributeKey.newInstance("opensearch-quic-server-ssl-engine");

    protected static final AttributeKey<Netty4HttpServerChannel> HTTP_SERVER_CHANNEL_KEY = AttributeKey.newInstance(
        "opensearch-quic-server-channel"
    );

    protected static class HttpChannelHandler extends ChannelInitializer<QuicStreamChannel> {

        private final Netty4Http3ServerTransport transport;
        private final NettyByteBufSizer byteBufSizer;
        private final Netty4Http3RequestCreator requestCreator;
        private final Netty4HttpRequestHandler requestHandler;
        private final Netty4HttpResponseCreator responseCreator;
        private final HttpHandlingSettings handlingSettings;

        protected HttpChannelHandler(final Netty4Http3ServerTransport transport, final HttpHandlingSettings handlingSettings) {
            this.transport = transport;
            this.handlingSettings = handlingSettings;
            this.byteBufSizer = new NettyByteBufSizer();
            this.requestCreator = new Netty4Http3RequestCreator(transport.maxInitialLineLength);
            this.requestHandler = new Netty4HttpRequestHandler(transport, HTTP_CHANNEL_KEY);
            this.responseCreator = new Netty4HttpResponseCreator();
        }

        public ChannelHandler getRequestHandler() {
            return requestHandler;
        }

        @Override
        protected void initChannel(QuicStreamChannel ch) throws Exception {
            final Netty4HttpChannel nettyHttpChannel = new Netty4HttpChannel(ch, HTTP_SERVER_ENGINE_KEY);
            ch.attr(HTTP_CHANNEL_KEY).set(nettyHttpChannel);
            ch.pipeline().addLast("byte_buf_sizer", byteBufSizer);
            ch.pipeline().addLast("read_timeout", new ReadTimeoutHandler(transport.readTimeoutMillis, TimeUnit.MILLISECONDS));

            configurePipeline(ch);
            transport.serverAcceptedChannel(nettyHttpChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);

            final Channel parent = parent(ctx.channel());
            final Channel channel = (parent != null) ? parent : ctx.channel();
            final Netty4HttpServerChannel httpServerChannel = channel.attr(HTTP_SERVER_CHANNEL_KEY).get();
            if (cause instanceof Error) {
                transport.onServerException(httpServerChannel, new Exception(cause));
            } else {
                transport.onServerException(httpServerChannel, (Exception) cause);
            }
        }

        protected void configurePipeline(Channel ch) {
            ch.pipeline().addLast(new Http3FrameToHttpObjectCodec(true));
            ch.pipeline().addLast("header_verifier", transport.createHeaderVerifier());
            ch.pipeline().addLast("decoder_compress", transport.createDecompressor());
            final HttpObjectAggregator aggregator = new HttpObjectAggregator(handlingSettings.getMaxContentLength());
            aggregator.setMaxCumulationBufferComponents(transport.maxCompositeBufferComponents);
            ch.pipeline().addLast("aggregator", aggregator);
            if (handlingSettings.isCompression()) {
                ch.pipeline()
                    .addLast(
                        "encoder_compress",
                        new HttpContentCompressor(defaultCompressionOptions(handlingSettings.getCompressionLevel()))
                    );
            }
            ch.pipeline().addLast("request_creator", requestCreator);
            ch.pipeline().addLast("response_creator", responseCreator);
            ch.pipeline().addLast("pipelining", new Netty4HttpPipeliningHandler(logger, transport.pipeliningMaxEvents));
            ch.pipeline().addLast("handler", requestHandler);
        }

        private NioDatagramChannel parent(Channel channel) {
            if (channel == null) {
                return null;
            } else if (channel instanceof NioDatagramChannel ndc) {
                return ndc; /* parent server channel */
            } else {
                return parent(channel.parent());
            }
        }
    }

    /**
     * Extension point that allows a NetworkPlugin to extend the netty pipeline and inspect headers after request decoding
     */
    protected ChannelInboundHandlerAdapter createHeaderVerifier() {
        // pass-through
        return new ChannelInboundHandlerAdapter();
    }

    /**
     * Extension point that allows a NetworkPlugin to override the default netty HttpContentDecompressor and supply a custom decompressor.
     *
     * Used in instances to conditionally decompress depending on the outcome from header verification
     */
    protected ChannelInboundHandlerAdapter createDecompressor() {
        return new HttpContentDecompressor();
    }

    /**
     * Copy of {@link HttpContentCompressor} default compression options with ZSTD excluded:
     * although zstd-jni is on the classpath, {@link ZstdEncoder} requires direct buffers support
     * which by default {@link NettyAllocator} does not provide.
     *
     * @param compressionLevel
     *        {@code 1} yields the fastest compression and {@code 9} yields the
     *        best compression.  {@code 0} means no compression.  The default
     *        compression level is {@code 6}.
     *
     * @return default compression options
     */
    private static CompressionOptions[] defaultCompressionOptions(int compressionLevel) {
        return defaultCompressionOptions(compressionLevel, 15, 8);
    }

    /**
     * Copy of {@link HttpContentCompressor} default compression options with ZSTD excluded:
     * although zstd-jni is on the classpath, {@link ZstdEncoder} requires direct buffers support
     * which by default {@link NettyAllocator} does not provide.
     *
     * @param compressionLevel
     *        {@code 1} yields the fastest compression and {@code 9} yields the
     *        best compression.  {@code 0} means no compression.  The default
     *        compression level is {@code 6}.
     * @param windowBits
     *        The base two logarithm of the size of the history buffer.  The
     *        value should be in the range {@code 9} to {@code 15} inclusive.
     *        Larger values result in better compression at the expense of
     *        memory usage.  The default value is {@code 15}.
     * @param memLevel
     *        How much memory should be allocated for the internal compression
     *        state.  {@code 1} uses minimum memory and {@code 9} uses maximum
     *        memory.  Larger values result in better and faster compression
     *        at the expense of memory usage.  The default value is {@code 8}
     *
     * @return default compression options
     */
    private static CompressionOptions[] defaultCompressionOptions(int compressionLevel, int windowBits, int memLevel) {
        final List<CompressionOptions> options = new ArrayList<CompressionOptions>(4);
        final GzipOptions gzipOptions = StandardCompressionOptions.gzip(compressionLevel, windowBits, memLevel);
        final DeflateOptions deflateOptions = StandardCompressionOptions.deflate(compressionLevel, windowBits, memLevel);

        options.add(gzipOptions);
        options.add(deflateOptions);
        options.add(StandardCompressionOptions.snappy());

        if (Brotli.isAvailable()) {
            options.add(StandardCompressionOptions.brotli());
        }

        return options.toArray(new CompressionOptions[0]);
    }

}
