/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import io.grpc.BindableService;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.transport.grpc.Netty4GrpcServerTransport;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;

/**
 * Netty4GrpcServerTransport with TLS enabled.
 * Security settings injected through a SecureAuxTransportSettingsProvider.
 */
public class SecureNetty4GrpcServerTransport extends Netty4GrpcServerTransport {
    private static final Logger logger = LogManager.getLogger(SecureNetty4GrpcServerTransport.class);

    private final SecureAuxTransportSettingsProvider secureAuxTransportSettingsProvider;

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
        this.addServerConfig(
            (NettyServerBuilder builder) -> {
                try {
                    return builder.sslContext(buildSslContext());
                } catch (SSLException | NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }

    private SslContext buildSslContext() throws SSLException, NoSuchAlgorithmException {
        Optional<SSLContext> SSLCtxt = secureAuxTransportSettingsProvider.buildSecureAuxServerSSLContext(
            this.settings,
            this
        );

        if (SSLCtxt.isPresent()) {
            return new SSLContextWrapper(SSLCtxt.get(), false);
        }

        return new SSLContextWrapper(false);
    }
}
