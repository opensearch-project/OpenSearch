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

import java.util.List;

import io.grpc.BindableService;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

/**
 * Netty4GrpcServerTransport with TLS enabled.
 * Security settings injected through a SecureAuxTransportSettingsProvider.
 */
public class SecureNetty4GrpcServerTransport extends Netty4GrpcServerTransport {
    private final SslContext sslContext;

    /**
     * Creates a new SecureNetty4GrpcServerTransport instance and inject a ReloadableSecureAuxTransportSslContext
     * into the NettyServerBuilder config to enable TLS on the server.
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
        this.port = SecureNetty4GrpcServerTransport.SETTING_GRPC_PORT.get(settings);
        this.sslContext = new ReloadableSecureAuxTransportSslContext(secureTransportSettingsProvider, false);
        this.addServerConfig((NettyServerBuilder builder) -> builder.sslContext(this.sslContext));
    }
}
