/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.net.ssl.SSLException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

/**
 * Clone of {@link org.apache.arrow.flight.FlightClient} to support setting SslContext and other settings like SslContext, workerELG,
 * executorService and channelType directly. It can be discarded once FlightClient.Builder supports setting SslContext directly.
 * Note: This file needs to be cloned with version upgrade of arrow flight-core with above changes.
 */
public class OSFlightClient {
    /** A builder for Flight clients. */
    public final static class Builder {
        private BufferAllocator allocator;
        private Location location;
        private boolean forceTls = false;
        private int maxInboundMessageSize = OSFlightServer.MAX_GRPC_MESSAGE_SIZE;
        private InputStream trustedCertificates = null;
        private InputStream clientCertificate = null;
        private InputStream clientKey = null;
        private String overrideHostname = null;
        private List<FlightClientMiddleware.Factory> middleware = new ArrayList<>();
        private boolean verifyServer = true;

        private EventLoopGroup workerELG;
        private ExecutorService executorService;
        private Class<? extends io.netty.channel.Channel> channelType;
        private SslContext sslContext;
        
        private Builder() {}

        Builder(BufferAllocator allocator, Location location) {
            this.allocator = Preconditions.checkNotNull(allocator);
            this.location = Preconditions.checkNotNull(location);
        }

        /** Force the client to connect over TLS. */
        public Builder useTls() {
            this.forceTls = true;
            return this;
        }

        /** Override the hostname checked for TLS. Use with caution in production. */
        public Builder overrideHostname(final String hostname) {
            this.overrideHostname = hostname;
            return this;
        }

        /** Set the maximum inbound message size. */
        public Builder maxInboundMessageSize(int maxSize) {
            Preconditions.checkArgument(maxSize > 0);
            this.maxInboundMessageSize = maxSize;
            return this;
        }

        /** Set the trusted TLS certificates. */
        public Builder trustedCertificates(final InputStream stream) {
            this.trustedCertificates = Preconditions.checkNotNull(stream);
            return this;
        }

        /** Set the trusted TLS certificates. */
        public Builder clientCertificate(
            final InputStream clientCertificate, final InputStream clientKey) {
            Preconditions.checkNotNull(clientKey);
            this.clientCertificate = Preconditions.checkNotNull(clientCertificate);
            this.clientKey = Preconditions.checkNotNull(clientKey);
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

        public Builder intercept(FlightClientMiddleware.Factory factory) {
            middleware.add(factory);
            return this;
        }

        public Builder verifyServer(boolean verifyServer) {
            this.verifyServer = verifyServer;
            return this;
        }

        /** Create the client from this builder. */
        public FlightClient build() {
            final NettyChannelBuilder builder;

            switch (location.getUri().getScheme()) {
                case LocationSchemes.GRPC:
                case LocationSchemes.GRPC_INSECURE:
                case LocationSchemes.GRPC_TLS:
                {
                    builder = NettyChannelBuilder.forAddress(location.toSocketAddress());
                    break;
                }
                case LocationSchemes.GRPC_DOMAIN_SOCKET:
                {
                    // The implementation is platform-specific, so we have to find the classes at runtime
                    builder = NettyChannelBuilder.forAddress(location.toSocketAddress());
                    try {
                        try {
                            // Linux
                            builder.channelType(
                                Class.forName("io.netty.channel.epoll.EpollDomainSocketChannel")
                                    .asSubclass(ServerChannel.class));
                            final EventLoopGroup elg =  
                                Class.forName("io.netty.channel.epoll.EpollEventLoopGroup")
                                    .asSubclass(EventLoopGroup.class)
                                    .getDeclaredConstructor()
                                    .newInstance();
                            builder.eventLoopGroup(elg);
                        } catch (ClassNotFoundException e) {
                            // BSD
                            builder.channelType(
                                Class.forName("io.netty.channel.kqueue.KQueueDomainSocketChannel")
                                    .asSubclass(ServerChannel.class));
                            final EventLoopGroup elg =  
                                Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup")
                                    .asSubclass(EventLoopGroup.class)
                                    .getDeclaredConstructor()
                                    .newInstance();
                            builder.eventLoopGroup(elg);
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
                default:
                    throw new IllegalArgumentException(
                        "Scheme is not supported: " + location.getUri().getScheme());
            }

            if (this.forceTls || LocationSchemes.GRPC_TLS.equals(location.getUri().getScheme())) {
                builder.useTransportSecurity();

                final boolean hasTrustedCerts = this.trustedCertificates != null;
                final boolean hasKeyCertPair = this.clientCertificate != null && this.clientKey != null;
                if (!this.verifyServer && (hasTrustedCerts || hasKeyCertPair)) {
                    throw new IllegalArgumentException(
                        "FlightClient has been configured to disable server verification, "
                            + "but certificate options have been specified.");
                }

                if (sslContext != null) {
                    builder.sslContext(sslContext);
                } else {
                    final SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
    
                    if (!this.verifyServer) {
                        sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                    } else if (this.trustedCertificates != null
                        || this.clientCertificate != null
                        || this.clientKey != null) {
                        if (this.trustedCertificates != null) {
                            sslContextBuilder.trustManager(this.trustedCertificates);
                        }
                        if (this.clientCertificate != null && this.clientKey != null) {
                            sslContextBuilder.keyManager(this.clientCertificate, this.clientKey);
                        }
                    }
                    try {
                        builder.sslContext(sslContextBuilder.build());
                    } catch (SSLException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (this.overrideHostname != null) {
                    builder.overrideAuthority(this.overrideHostname);
                }
            } else {
                builder.usePlaintext();
            }

            builder
                .maxTraceEvents(0)
                .maxInboundMessageSize(maxInboundMessageSize)
                .maxInboundMetadataSize(maxInboundMessageSize)
                .executor(executorService);

            if (channelType != null) {
                builder.channelType(channelType);
            }
 
            if (workerELG != null) {
                builder.eventLoopGroup(workerELG);
            }
 
            return new FlightClient(allocator, builder.build(), middleware);
        }

        public Builder executor(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public Builder channelType(Class<? extends io.netty.channel.Channel> channelType) {
            this.channelType = channelType;
            return this;
        }
        
        public Builder eventLoopGroup(EventLoopGroup workerELG) {
            this.workerELG = workerELG;
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
