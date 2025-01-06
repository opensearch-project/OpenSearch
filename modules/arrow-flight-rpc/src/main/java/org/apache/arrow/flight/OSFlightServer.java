/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.auth.ServerAuthInterceptor;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.ServerCallHeaderAuthMiddleware;
import org.apache.arrow.flight.grpc.ServerBackpressureThresholdInterceptor;
import org.apache.arrow.flight.grpc.ServerInterceptorAdapter;
import org.apache.arrow.flight.grpc.ServerInterceptorAdapter.KeyFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * Clone of {@link FlightServer} to support setting SslContext directly. It can be discarded once
 * FlightServer.Builder supports setting SslContext directly.
 */
public class OSFlightServer implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(OSFlightServer.class);

    private final Location location;
    private final Server server;
    // The executor used by the gRPC server. We don't use it here, but we do need to clean it up with
    // the server.
    // May be null, if a user-supplied executor was provided (as we do not want to clean that up)
    @VisibleForTesting
    final ExecutorService grpcExecutor;

    /** The maximum size of an individual gRPC message. This effectively disables the limit. */
    static final int MAX_GRPC_MESSAGE_SIZE = Integer.MAX_VALUE;

    /** The default number of bytes that can be queued on an output stream before blocking. */
    public static final int DEFAULT_BACKPRESSURE_THRESHOLD = 10 * 1024 * 1024; // 10MB

    /** Create a new instance from a gRPC server. For internal use only. */
    private OSFlightServer(Location location, Server server, ExecutorService grpcExecutor) {
        this.location = location;
        this.server = server;
        this.grpcExecutor = grpcExecutor;
    }

    /** Start the server. */
    public OSFlightServer start() throws IOException {
        server.start();
        return this;
    }

    /** Get the port the server is running on (if applicable). */
    public int getPort() {
        return server.getPort();
    }

    /** Get the location for this server. */
    public Location getLocation() {
        if (location.getUri().getPort() == 0) {
            // If the server was bound to port 0, replace the port in the location with the real port.
            final URI uri = location.getUri();
            try {
                return new Location(
                    new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), getPort(), uri.getPath(), uri.getQuery(), uri.getFragment())
                );
            } catch (URISyntaxException e) {
                // We don't expect this to happen
                throw new RuntimeException(e);
            }
        }
        return location;
    }

    /** Block until the server shuts down. */
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    /** Request that the server shut down. */
    public void shutdown() {
        server.shutdown();
        if (grpcExecutor != null) {
            grpcExecutor.shutdown();
        }
    }

    /**
     * Wait for the server to shut down with a timeout.
     *
     * @return true if the server shut down successfully.
     */
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        return server.awaitTermination(timeout, unit);
    }

    /** Shutdown the server, waits for up to 6 seconds for successful shutdown before returning. */
    @Override
    public void close() throws InterruptedException {
        shutdown();
        final boolean terminated = awaitTermination(3000, TimeUnit.MILLISECONDS);
        if (terminated) {
            logger.debug("Server was terminated within 3s");
            return;
        }

        // get more aggressive in termination.
        server.shutdownNow();

        int count = 0;
        while (!server.isTerminated() && count < 30) {
            count++;
            logger.debug("Waiting for termination");
            Thread.sleep(100);
        }

        if (!server.isTerminated()) {
            logger.warn("Couldn't shutdown server, resources likely will be leaked.");
        }
    }

    /** Create a builder for a Flight server. */
    public static Builder builder() {
        return new Builder();
    }

    /** Create a builder for a Flight server. */
    public static Builder builder(BufferAllocator allocator, Location location, FlightProducer producer) {
        return new Builder(allocator, location, producer);
    }

    public static Builder builder(
        BufferAllocator allocator,
        Location location,
        FlightProducer producer,
        SslContext sslContext,
        Class<? extends Channel> channelType,
        EventLoopGroup bossELG,
        EventLoopGroup workerELG,
        ExecutorService grpcExecutor
    ) {
        Builder builder = new Builder(allocator, location, producer);
        if (sslContext != null) {
            builder.useTls(sslContext);
        }
        builder.transportHint("netty.channelType", channelType);
        builder.transportHint("netty.bossEventLoopGroup", bossELG);
        builder.transportHint("netty.workerEventLoopGroup", workerELG);
        builder.executor(grpcExecutor);
        return builder;
    }

    /** A builder for Flight servers. */
    public static final class Builder {
        private BufferAllocator allocator;
        private Location location;
        private FlightProducer producer;
        private final Map<String, Object> builderOptions;
        private ServerAuthHandler authHandler = ServerAuthHandler.NO_OP;
        private CallHeaderAuthenticator headerAuthenticator = CallHeaderAuthenticator.NO_OP;
        private ExecutorService executor = null;
        private int maxInboundMessageSize = MAX_GRPC_MESSAGE_SIZE;
        private int maxHeaderListSize = MAX_GRPC_MESSAGE_SIZE;
        private int backpressureThreshold = DEFAULT_BACKPRESSURE_THRESHOLD;
        private InputStream certChain;
        private InputStream key;
        private InputStream mTlsCACert;
        private SslContext sslContext;
        private final List<KeyFactory<?>> interceptors;
        // Keep track of inserted interceptors
        private final Set<String> interceptorKeys;

        Builder() {
            builderOptions = new HashMap<>();
            interceptors = new ArrayList<>();
            interceptorKeys = new HashSet<>();
        }

        Builder(BufferAllocator allocator, Location location, FlightProducer producer) {
            this();
            this.allocator = Preconditions.checkNotNull(allocator);
            this.location = Preconditions.checkNotNull(location);
            this.producer = Preconditions.checkNotNull(producer);
        }

        /** Create the server for this builder. */
        @SuppressWarnings("unchecked")
        public OSFlightServer build() {
            // Add the auth middleware if applicable.
            if (headerAuthenticator != CallHeaderAuthenticator.NO_OP) {
                this.middleware(
                    FlightServerMiddleware.Key.of(Auth2Constants.AUTHORIZATION_HEADER),
                    new ServerCallHeaderAuthMiddleware.Factory(headerAuthenticator)
                );
            }

            this.middleware(FlightConstants.HEADER_KEY, new ServerHeaderMiddleware.Factory());

            final NettyServerBuilder builder;
            switch (location.getUri().getScheme()) {
                case LocationSchemes.GRPC_DOMAIN_SOCKET: {
                    // The implementation is platform-specific, so we have to find the classes at runtime
                    builder = NettyServerBuilder.forAddress(location.toSocketAddress());
                    try {
                        try {
                            // Linux
                            builder.channelType(
                                Class.forName("io.netty.channel.epoll.EpollServerDomainSocketChannel").asSubclass(ServerChannel.class)
                            );
                            final EventLoopGroup elg = Class.forName("io.netty.channel.epoll.EpollEventLoopGroup")
                                .asSubclass(EventLoopGroup.class)
                                .getConstructor()
                                .newInstance();
                            builder.bossEventLoopGroup(elg).workerEventLoopGroup(elg);
                        } catch (ClassNotFoundException e) {
                            // BSD
                            builder.channelType(
                                Class.forName("io.netty.channel.kqueue.KQueueServerDomainSocketChannel").asSubclass(ServerChannel.class)
                            );
                            final EventLoopGroup elg = Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup")
                                .asSubclass(EventLoopGroup.class)
                                .getConstructor()
                                .newInstance();
                            builder.bossEventLoopGroup(elg).workerEventLoopGroup(elg);
                        }
                    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                        | InvocationTargetException e) {
                        throw new UnsupportedOperationException(
                            "Could not find suitable Netty native transport implementation for domain socket address."
                        );
                    }
                    break;
                }
                case LocationSchemes.GRPC:
                case LocationSchemes.GRPC_INSECURE: {
                    builder = NettyServerBuilder.forAddress(location.toSocketAddress());
                    break;
                }
                case LocationSchemes.GRPC_TLS: {
                    if (certChain == null) {
                        throw new IllegalArgumentException("Must provide a certificate and key to serve gRPC over TLS");
                    }
                    builder = NettyServerBuilder.forAddress(location.toSocketAddress());
                    break;
                }
                default:
                    throw new IllegalArgumentException("Scheme is not supported: " + location.getUri().getScheme());
            }

            if (sslContext != null && certChain != null) {
                SslContextBuilder sslContextBuilder = GrpcSslContexts.forServer(certChain, key);

                if (mTlsCACert != null) {
                    sslContextBuilder.clientAuth(ClientAuth.REQUIRE).trustManager(mTlsCACert);
                }
                try {
                    sslContext = sslContextBuilder.build();
                } catch (SSLException e) {
                    throw new RuntimeException(e);
                } finally {
                    closeMTlsCACert();
                    closeCertChain();
                    closeKey();
                }

                builder.sslContext(sslContext);
            }

            // Share one executor between the gRPC service, DoPut, and Handshake
            final ExecutorService exec;
            // We only want to have FlightServer close the gRPC executor if we created it here. We should
            // not close
            // user-supplied executors.
            final ExecutorService grpcExecutor;
            if (executor != null) {
                exec = executor;
                grpcExecutor = null;
            } else {
                throw new IllegalStateException("GRPC executor must be passed to start Flight server.");
            }

            final FlightBindingService flightService = new FlightBindingService(allocator, producer, authHandler, exec);
            builder.executor(exec)
                .maxInboundMessageSize(maxInboundMessageSize)
                .maxInboundMetadataSize(maxHeaderListSize)
                .addService(
                    ServerInterceptors.intercept(
                        flightService,
                        new ServerBackpressureThresholdInterceptor(backpressureThreshold),
                        new ServerAuthInterceptor(authHandler)
                    )
                );

            // Allow hooking into the gRPC builder. This is not guaranteed to be available on all Arrow
            // versions or
            // Flight implementations.
            builderOptions.computeIfPresent("grpc.builderConsumer", (key, builderConsumer) -> {
                final Consumer<NettyServerBuilder> consumer = (Consumer<NettyServerBuilder>) builderConsumer;
                consumer.accept(builder);
                return null;
            });

            // Allow explicitly setting some Netty-specific options
            builderOptions.computeIfPresent("netty.channelType", (key, channelType) -> {
                builder.channelType((Class<? extends ServerChannel>) channelType);
                return null;
            });
            builderOptions.computeIfPresent("netty.bossEventLoopGroup", (key, elg) -> {
                builder.bossEventLoopGroup((EventLoopGroup) elg);
                return null;
            });
            builderOptions.computeIfPresent("netty.workerEventLoopGroup", (key, elg) -> {
                builder.workerEventLoopGroup((EventLoopGroup) elg);
                return null;
            });

            builder.intercept(new ServerInterceptorAdapter(interceptors));
            return new OSFlightServer(location, builder.build(), grpcExecutor);
        }

        public Builder setMaxHeaderListSize(int maxHeaderListSize) {
            this.maxHeaderListSize = maxHeaderListSize;
            return this;
        }

        /**
         * Set the maximum size of a message. Defaults to "unlimited", depending on the underlying
         * transport.
         */
        public Builder maxInboundMessageSize(int maxMessageSize) {
            this.maxInboundMessageSize = maxMessageSize;
            return this;
        }

        /**
         * Set the number of bytes that may be queued on a server output stream before writes are
         * blocked.
         */
        public Builder backpressureThreshold(int backpressureThreshold) {
            Preconditions.checkArgument(backpressureThreshold > 0);
            this.backpressureThreshold = backpressureThreshold;
            return this;
        }

        /**
         * A small utility function to ensure that InputStream attributes. are closed if they are not
         * null
         *
         * @param stream The InputStream to close (if it is not null).
         */
        private void closeInputStreamIfNotNull(InputStream stream) {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException expected) {
                    // stream closes gracefully, doesn't expect an exception.
                }
            }
        }

        /**
         * A small utility function to ensure that the certChain attribute is closed if it is not null.
         * It then sets the attribute to null.
         */
        private void closeCertChain() {
            closeInputStreamIfNotNull(certChain);
            certChain = null;
        }

        /**
         * A small utility function to ensure that the key attribute is closed if it is not null. It
         * then sets the attribute to null.
         */
        private void closeKey() {
            closeInputStreamIfNotNull(key);
            key = null;
        }

        /**
         * A small utility function to ensure that the mTlsCACert attribute is closed if it is not null.
         * It then sets the attribute to null.
         */
        private void closeMTlsCACert() {
            closeInputStreamIfNotNull(mTlsCACert);
            mTlsCACert = null;
        }

        /**
         * Enable TLS on the server.
         *
         * @param certChain The certificate chain to use.
         * @param key The private key to use.
         */
        public Builder useTls(final File certChain, final File key) throws IOException {
            closeCertChain();
            this.certChain = new FileInputStream(certChain);

            closeKey();
            this.key = new FileInputStream(key);

            return this;
        }

        /**
         * Enable Client Verification via mTLS on the server.
         *
         * @param mTlsCACert The CA certificate to use for verifying clients.
         */
        public Builder useMTlsClientVerification(final File mTlsCACert) throws IOException {
            closeMTlsCACert();
            this.mTlsCACert = new FileInputStream(mTlsCACert);
            return this;
        }

        /**
         * Enable TLS on the server.
         *
         * @param certChain The certificate chain to use.
         * @param key The private key to use.
         */
        public Builder useTls(final InputStream certChain, final InputStream key) throws IOException {
            closeCertChain();
            this.certChain = certChain;

            closeKey();
            this.key = key;

            return this;
        }

        /**
         * Enable TLS on the server.
         * @param sslContext SslContext to use.
         */
        public Builder useTls(SslContext sslContext) {
            this.sslContext = Objects.requireNonNull(sslContext);
            return this;
        }

        /**
         * Enable mTLS on the server.
         *
         * @param mTlsCACert The CA certificate to use for verifying clients.
         */
        public Builder useMTlsClientVerification(final InputStream mTlsCACert) throws IOException {
            closeMTlsCACert();
            this.mTlsCACert = mTlsCACert;
            return this;
        }

        /**
         * Set the executor used by the server.
         *
         * <p>Flight will NOT take ownership of the executor. The application must clean it up if one is
         * provided. (If not provided, Flight will use a default executor which it will clean up.)
         */
        public Builder executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /** Set the authentication handler. */
        public Builder authHandler(ServerAuthHandler authHandler) {
            this.authHandler = authHandler;
            return this;
        }

        /** Set the header-based authentication mechanism. */
        public Builder headerAuthenticator(CallHeaderAuthenticator headerAuthenticator) {
            this.headerAuthenticator = headerAuthenticator;
            return this;
        }

        /** Provide a transport-specific option. Not guaranteed to have any effect. */
        public Builder transportHint(final String key, Object option) {
            builderOptions.put(key, option);
            return this;
        }

        /**
         * Add a Flight middleware component to inspect and modify requests to this service.
         *
         * @param key An identifier for this middleware component. Service implementations can retrieve
         *     the middleware instance for the current call using {@link
         *     org.apache.arrow.flight.FlightProducer.CallContext}.
         * @param factory A factory for the middleware.
         * @param <T> The middleware type.
         * @throws IllegalArgumentException if the key already exists
         */
        public <T extends FlightServerMiddleware> Builder middleware(
            final FlightServerMiddleware.Key<T> key,
            final FlightServerMiddleware.Factory<T> factory
        ) {
            if (interceptorKeys.contains(key.key)) {
                throw new IllegalArgumentException("Key already exists: " + key.key);
            }
            interceptors.add(new KeyFactory<>(key, factory));
            interceptorKeys.add(key.key);
            return this;
        }

        public Builder allocator(BufferAllocator allocator) {
            this.allocator = Preconditions.checkNotNull(allocator);
            return this;
        }

        public Builder location(Location location) {
            this.location = Preconditions.checkNotNull(location);
            return this;
        }

        public Builder producer(FlightProducer producer) {
            this.producer = Preconditions.checkNotNull(producer);
            return this;
        }
    }
}
