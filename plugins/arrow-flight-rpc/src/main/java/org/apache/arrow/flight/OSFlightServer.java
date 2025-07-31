/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import javax.net.ssl.SSLException;

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

/**
 * Clone of {@link org.apache.arrow.flight.FlightServer} to support setting SslContext. It can be discarded once FlightServer.Builder supports setting SslContext directly.
 * <p>
 * It changes {@link org.apache.arrow.flight.FlightServer.Builder} to allow hook to configure the NettyServerBuilder.
 */
@SuppressWarnings("removal")
public class OSFlightServer {
    /** The maximum size of an individual gRPC message. This effectively disables the limit. */
    static final int MAX_GRPC_MESSAGE_SIZE = Integer.MAX_VALUE;
    /** The default number of bytes that can be queued on an output stream before blocking. */
    static final int DEFAULT_BACKPRESSURE_THRESHOLD = 10 * 1024 * 1024; // 10MB

    private static final MethodHandle FLIGHT_SERVER_CTOR_MH;

    static {
            FLIGHT_SERVER_CTOR_MH = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
                    try {
                return MethodHandles
                .privateLookupIn(FlightServer.class, MethodHandles.lookup())
                .findConstructor(FlightServer.class, MethodType.methodType(void.class, Location.class, Server.class, ExecutorService.class));
        } catch (final NoSuchMethodException | IllegalAccessException ex) {
            throw new IllegalStateException("Unable to find the FlightServer constructor to invoke", ex);
        }}
        );
    }

    /** A builder for Flight servers. */
    public final static class Builder {
        private BufferAllocator allocator;
        private Location location;
        private final List<Location> listenAddresses = new ArrayList<>();
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
        public FlightServer build() {
            // Add the auth middleware if applicable.
            if (headerAuthenticator != CallHeaderAuthenticator.NO_OP) {
                this.middleware(
                    FlightServerMiddleware.Key.of(Auth2Constants.AUTHORIZATION_HEADER),
                    new ServerCallHeaderAuthMiddleware.Factory(headerAuthenticator));
            }

            this.middleware(FlightConstants.HEADER_KEY, new ServerHeaderMiddleware.Factory());

            final NettyServerBuilder builder;
            
            // Use primary location for initial setup
            Location primaryLocation = location != null ? location : listenAddresses.get(0);
            
            switch (primaryLocation.getUri().getScheme()) {
                case LocationSchemes.GRPC_DOMAIN_SOCKET:
                {
                    // The implementation is platform-specific, so we have to find the classes at runtime
                    builder = NettyServerBuilder.forAddress(primaryLocation.toSocketAddress());
                    try {
                        try {
                            // Linux
                            builder.channelType(
                                Class.forName("io.netty.channel.epoll.EpollServerDomainSocketChannel")
                                    .asSubclass(ServerChannel.class));
                            final EventLoopGroup elg =
                                Class.forName("io.netty.channel.epoll.EpollEventLoopGroup")
                                    .asSubclass(EventLoopGroup.class)
                                    .getConstructor()
                                    .newInstance();
                            builder.bossEventLoopGroup(elg).workerEventLoopGroup(elg);
                        } catch (ClassNotFoundException e) {
                            // BSD
                            builder.channelType(
                                Class.forName("io.netty.channel.kqueue.KQueueServerDomainSocketChannel")
                                    .asSubclass(ServerChannel.class));
                            final EventLoopGroup elg =
                                Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup")
                                    .asSubclass(EventLoopGroup.class)
                                    .getConstructor()
                                    .newInstance();
                            builder.bossEventLoopGroup(elg).workerEventLoopGroup(elg);
                        }
                    } catch (ClassNotFoundException
                             | InstantiationException
                             | IllegalAccessException
                             | NoSuchMethodException
                             | InvocationTargetException e) {
                        throw new UnsupportedOperationException(
                            "Could not find suitable Netty native transport implementation for domain socket address.");
                    }
                    break;
                }
                case LocationSchemes.GRPC:
                case LocationSchemes.GRPC_INSECURE:
                {
                    builder = NettyServerBuilder.forAddress(primaryLocation.toSocketAddress());
                    break;
                }
                case LocationSchemes.GRPC_TLS:
                {
                    if (certChain == null && sslContext == null) {
                        throw new IllegalArgumentException(
                            "Must provide a certificate and key to serve gRPC over TLS");
                    }
                    builder = NettyServerBuilder.forAddress(primaryLocation.toSocketAddress());
                    break;
                }
                default:
                    throw new IllegalArgumentException(
                        "Scheme is not supported: " + primaryLocation.getUri().getScheme());
            }

            if (certChain != null && sslContext == null) {
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
            } else if (sslContext != null) {
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

            final FlightBindingService flightService =
                new FlightBindingService(allocator, producer, authHandler, exec);
            builder
                .executor(exec)
                .maxInboundMessageSize(maxInboundMessageSize)
                .maxInboundMetadataSize(maxHeaderListSize)
                .addService(
                    ServerInterceptors.intercept(
                        flightService,
                        new ServerBackpressureThresholdInterceptor(backpressureThreshold),
                        new ServerAuthInterceptor(authHandler)));

            // Allow hooking into the gRPC builder. This is not guaranteed to be available on all Arrow
            // versions or
            // Flight implementations.
            builderOptions.computeIfPresent(
                "grpc.builderConsumer",
                (key, builderConsumer) -> {
                    final Consumer<NettyServerBuilder> consumer =
                        (Consumer<NettyServerBuilder>) builderConsumer;
                    consumer.accept(builder);
                    return null;
                });

            // Allow explicitly setting some Netty-specific options
            builderOptions.computeIfPresent(
                "netty.channelType",
                (key, channelType) -> {
                    builder.channelType((Class<? extends ServerChannel>) channelType);
                    return null;
                });
            builderOptions.computeIfPresent(
                "netty.bossEventLoopGroup",
                (key, elg) -> {
                    builder.bossEventLoopGroup((EventLoopGroup) elg);
                    return null;
                });
            builderOptions.computeIfPresent(
                "netty.workerEventLoopGroup",
                (key, elg) -> {
                    builder.workerEventLoopGroup((EventLoopGroup) elg);
                    return null;
                });

            // Add additional listen addresses
            for (Location listenAddress : listenAddresses) {
                if (!listenAddress.equals(primaryLocation)) {
                    builder.addListenAddress(listenAddress.toSocketAddress());
                }
            }
            
            builder.intercept(new ServerInterceptorAdapter(interceptors));

            try {
                return (FlightServer)FLIGHT_SERVER_CTOR_MH.invoke(primaryLocation, builder.build(), grpcExecutor);
            } catch (final Throwable ex) {
                throw new IllegalStateException("Unable to instantiate FlightServer", ex);
            }
        }

        public Builder channelType(Class<? extends io.netty.channel.Channel> channelType) {
            builderOptions.put("netty.channelType", channelType);
            return this;
        }

        public Builder workerEventLoopGroup(EventLoopGroup workerELG) {
            builderOptions.put("netty.workerEventLoopGroup", workerELG);
            return this;
        }

        public Builder bossEventLoopGroup(EventLoopGroup bossELG) {
            builderOptions.put("netty.bossEventLoopGroup", bossELG);
            return this;
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
            final FlightServerMiddleware.Key<T> key, final FlightServerMiddleware.Factory<T> factory) {
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
        
        public Builder addListenAddress(Location location) {
            this.listenAddresses.add(Preconditions.checkNotNull(location));
            return this;
        }

        public Builder producer(FlightProducer producer) {
            this.producer = Preconditions.checkNotNull(producer);
            return this;
        }

        public Builder sslContext(SslContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
