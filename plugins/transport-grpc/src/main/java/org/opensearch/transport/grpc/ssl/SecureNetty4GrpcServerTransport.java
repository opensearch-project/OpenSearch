/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.transport.grpc.Netty4GrpcServerTransport;

import javax.net.ssl.SSLException;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import io.grpc.BindableService;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;

/**
 * Netty4GrpcServerTransport with TLS enabled.
 * Security settings injected through a SecureAuxTransportSettingsProvider.
 */
public class SecureNetty4GrpcServerTransport extends Netty4GrpcServerTransport {
    private final SecureAuxTransportSettingsProvider secureAuxTransportSettingsProvider;
    private final SslContext sslContext;

    /**
     * Creates a new SecureNetty4GrpcServerTransport instance.
     * @param settings the configured settings.
     * @param services the gRPC compatible services to be registered with the server.
     * @param networkService the bind/publish addresses.
     * @param secureTransportSettingsProvider TLS configuration settings.
     */
    public SecureNetty4GrpcServerTransport(
        Settings settings,
        List<BindableService> services,
        NetworkService networkService,
        SecureAuxTransportSettingsProvider secureTransportSettingsProvider
    ) {
        super(settings, services, networkService);
        this.secureAuxTransportSettingsProvider = secureTransportSettingsProvider;
        this.port = SecureNetty4GrpcServerTransport.SETTING_GRPC_PORT.get(settings);

        try {
            this.sslContext = buildSslContext();
        } catch (SSLException e) {
            throw new RuntimeException(SecureNetty4GrpcServerTransport.class + " failed to build SslContext", e);
        }

        this.addServerConfig((NettyServerBuilder builder) -> builder.sslContext(this.sslContext));
    }

    /**
     * @return io.grpc SslContext from SecureAuxTransportSettingsProvider.
     */
    private SslContext buildSslContext() throws SSLException {
        Optional<SecureAuxTransportSettingsProvider.SSLContextBuilder> optBuilder = secureAuxTransportSettingsProvider
            .getSSLContextBuilder();
        if (optBuilder.isEmpty()) {
            throw new SSLException("SSLContext could not be built from SecureAuxTransportSettingsProvider: provider empty");
        }

        SecureAuxTransportSettingsProvider.SSLContextBuilder ctxtBuilder = optBuilder.get();
        if (ctxtBuilder.getKeyManagerFactory().isEmpty()
            || ctxtBuilder.getTrustManagerFactory().isEmpty()
            || ctxtBuilder.getSslProvider().isEmpty()
            || ctxtBuilder.getClientAuth().isEmpty()) {
            throw new SSLException("SSLContext could not be built from SecureAuxTransportSettingsProvider: missing parameters");
        }

        return SslContextBuilder.forServer(ctxtBuilder.getKeyManagerFactory().get())
            .trustManager(ctxtBuilder.getTrustManagerFactory().get())
            .sslProvider(SslProvider.valueOf(ctxtBuilder.getSslProvider().get().toUpperCase(Locale.ROOT)))
            .clientAuth(ClientAuth.valueOf(ctxtBuilder.getClientAuth().get().toUpperCase(Locale.ROOT)))
            .protocols(ctxtBuilder.getProtocols())
            .ciphers(ctxtBuilder.getCipherSuites())
            .applicationProtocolConfig(
                new ApplicationProtocolConfig(
                    ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    ApplicationProtocolNames.HTTP_2
                )
            )
            .build();
    }
}
