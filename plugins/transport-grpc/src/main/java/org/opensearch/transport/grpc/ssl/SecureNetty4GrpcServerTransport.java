/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.transport.grpc.Netty4GrpcServerTransport;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;

import io.grpc.BindableService;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

/**
 * Netty4GrpcServerTransport with TLS enabled.
 * Security settings injected through a SecureAuxTransportSettingsProvider.
 */
public class SecureNetty4GrpcServerTransport extends Netty4GrpcServerTransport {
    private static final Logger logger = LogManager.getLogger(SecureNetty4GrpcServerTransport.class);

    private final SecureAuxTransportSettingsProvider secureAuxTransportSettingsProvider;

    /**
     * Hide parent GRPC_TRANSPORT_SETTING_KEY and SETTING_GRPC_PORT.
     * Overwrite port in constructor with configuration as specified by
     * SecureNetty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY and
     * SecureNetty4GrpcServerTransport.SETTING_GRPC_PORT.
     */
    public static final String GRPC_TRANSPORT_SETTING_KEY = "experimental-secure-transport-grpc";
    public static final Setting<PortsRange> SETTING_GRPC_PORT = AUX_TRANSPORT_PORT.getConcreteSettingForNamespace(
        GRPC_TRANSPORT_SETTING_KEY
    );

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

        this.addServerConfig((NettyServerBuilder builder) -> {
            try {
                return builder.sslContext(buildSslContext());
            } catch (SSLException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private SslContext buildSslContext() throws SSLException, NoSuchAlgorithmException {
        Optional<SSLContext> SSLCtxt = secureAuxTransportSettingsProvider.buildSecureAuxServerSSLContext(this.settings, this);

        if (SSLCtxt.isPresent()) {
            return new SSLContextWrapper(SSLCtxt.get(), false);
        }

        return new SSLContextWrapper(false);
    }
}
