/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.grpc.Netty4GrpcServerTransport;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.JdkSslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SupportedCipherSuiteFilter;

/**
 * Netty4GrpcServerTransport with TLS enabled.
 * Security settings injected through a SecureAuxTransportSettingsProvider.
 */
public class SecureNetty4GrpcServerTransport extends Netty4GrpcServerTransport {
    private static final String[] DEFAULT_SSL_PROTOCOLS = { "TLSv1.3", "TLSv1.2", "TLSv1.1" };

    /**
     * Type key to select secure transport.
     */
    public static final String GRPC_SECURE_TRANSPORT_SETTING_KEY = "secure-transport-grpc";

    /**
     * Distinct port setting required as it depends on transport type key.
     */
    public static final Setting<PortsRange> SETTING_GRPC_SECURE_PORT = AUX_TRANSPORT_PORT.getConcreteSettingForNamespace(
        GRPC_SECURE_TRANSPORT_SETTING_KEY
    );

    /**
     * In the case no SecureAuxTransportParameters restrict client auth mode to REQUIRE.
     * Assume no enabled cipher suites. Allow ssl context implementation to select defaults.
     */
    private static class DefaultParameters implements SecureAuxTransportSettingsProvider.SecureAuxTransportParameters {
        @Override
        public Optional<String> clientAuth() {
            return Optional.of(ClientAuth.REQUIRE.name());
        }

        @Override
        public Collection<String> cipherSuites() {
            return List.of();
        }
    }

    /**
     * Creates a new SecureNetty4GrpcServerTransport instance and inject a SecureAuxTransportSslContext
     * into the NettyServerBuilder config to enable TLS on the server.
     * @param settings the configured settings.
     * @param services the gRPC compatible services to be registered with the server.
     * @param networkService the bind/publish addresses.
     * @param threadPool the thread pool for managing gRPC executor and monitoring.
     * @param secureTransportSettingsProvider TLS configuration settings.
     * @param serverInterceptor the gRPC server interceptor to be registered with the server.
     */
    public SecureNetty4GrpcServerTransport(
        Settings settings,
        List<BindableService> services,
        NetworkService networkService,
        ThreadPool threadPool,
        SecureAuxTransportSettingsProvider secureTransportSettingsProvider,
        ServerInterceptor serverInterceptor
    ) {
        super(settings, services, networkService, threadPool, serverInterceptor);
        this.port = SecureNetty4GrpcServerTransport.SETTING_GRPC_SECURE_PORT.get(settings);
        this.portSettingKey = SecureNetty4GrpcServerTransport.SETTING_GRPC_SECURE_PORT.getKey();
        try {
            JdkSslContext ctxt = getSslContext(settings, secureTransportSettingsProvider);
            this.addServerConfig((NettyServerBuilder builder) -> builder.sslContext(ctxt));
        } catch (Exception e) {
            throw new RuntimeException("Failed to build SslContext for " + SecureNetty4GrpcServerTransport.class.getName(), e);
        }
    }

    @Override
    public String settingKey() {
        return GRPC_SECURE_TRANSPORT_SETTING_KEY;
    }

    /**
     * Construct JdkSslContext, wrapping javax SSLContext as supplied by SecureAuxTransportSettingsProvider with applied
     * configurations settings in SecureAuxTransportParameters for this transport.
     * If optional SSLContext is empty, use default context as configured through JDK.
     * If SecureAuxTransportParameters empty, set ClientAuth OPTIONAL and allow all default supported ciphers.
     * @param settings the configured settings.
     * @param provider for SSLContext and SecureAuxTransportParameters (ClientAuth and enabled ciphers).
     */
    private JdkSslContext getSslContext(Settings settings, SecureAuxTransportSettingsProvider provider) throws SSLException {
        Optional<SSLContext> sslContext = provider.buildSecureAuxServerTransportContext(settings, this.settingKey());
        if (sslContext.isEmpty()) {
            try {
                sslContext = Optional.of(SSLContext.getDefault());
            } catch (NoSuchAlgorithmException e) {
                throw new SSLException("Failed to build default SSLContext for " + SecureNetty4GrpcServerTransport.class.getName(), e);
            }
        }
        SecureAuxTransportSettingsProvider.SecureAuxTransportParameters params = provider.parameters(settings, this.settingKey())
            .orElseGet(DefaultParameters::new);
        ClientAuth clientAuth = ClientAuth.valueOf(params.clientAuth().orElseThrow().toUpperCase(Locale.ROOT));
        return new JdkSslContext(
            sslContext.get(),
            false,
            params.cipherSuites(),
            SupportedCipherSuiteFilter.INSTANCE,
            new ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                ApplicationProtocolNames.HTTP_2 // gRPC -> always http2
            ),
            clientAuth,
            DEFAULT_SSL_PROTOCOLS,
            true
        );
    }
}
