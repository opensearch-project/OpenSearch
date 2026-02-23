/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.SecureTransportSettingsProvider;

import javax.net.ssl.SSLException;

import java.util.Locale;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Default {@link SslContextProvider} that wraps TLS contexts in {@link ReloadableSslContext}.
 * <p>
 * Each {@link ReloadableSslContext} holds a supplier that calls
 * {@link SecureTransportSettingsProvider#parameters(Settings)} on every {@code newEngine()} invocation.
 * Since {@code parameters()} reads from the live {@code SslContextHandler.sslContext} field — the same
 * field the security plugin's {@code reloadcerts} API replaces — any new TCP connection after a cert
 * reload automatically uses the updated certificate material. No explicit reload hook or listener is
 * needed; this is the same mechanism that makes {@code SecureNetty4Transport} work.
 */
public class DefaultSslContextProvider implements SslContextProvider {

    private final ReloadableSslContext serverSslContext;
    private final ReloadableSslContext clientSslContext;

    /**
     * Creates a new {@link DefaultSslContextProvider} wrapping server and client {@link ReloadableSslContext}
     * instances built from the given {@link SecureTransportSettingsProvider} and {@link Settings}.
     *
     * @param secureTransportSettingsProvider provides TLS parameters (keys, certs, ciphers, protocols)
     * @param settings node settings used for hostname verification and other transport options
     */
    public DefaultSslContextProvider(SecureTransportSettingsProvider secureTransportSettingsProvider, Settings settings) {
        SslContext initialServer = buildServerSslContext(secureTransportSettingsProvider, settings);
        this.serverSslContext = new ReloadableSslContext(
            initialServer,
            () -> buildServerSslContext(secureTransportSettingsProvider, settings)
        );

        SslContext initialClient = buildClientSslContext(secureTransportSettingsProvider, settings);
        this.clientSslContext = new ReloadableSslContext(
            initialClient,
            () -> buildClientSslContext(secureTransportSettingsProvider, settings)
        );
    }

    /** {@inheritDoc} */
    @Override
    public SslContext getServerSslContext() {
        return serverSslContext;
    }

    /** {@inheritDoc} */
    @Override
    public SslContext getClientSslContext() {
        return clientSslContext;
    }

    private static SslContext buildServerSslContext(SecureTransportSettingsProvider provider, Settings settings) {
        try {
            SecureTransportSettingsProvider.SecureTransportParameters parameters = provider.parameters(null).get();
            return SslContextBuilder.forServer(parameters.keyManagerFactory().get())
                .sslProvider(SslProvider.valueOf(parameters.sslProvider().get().toUpperCase(Locale.ROOT)))
                .clientAuth(ClientAuth.valueOf(parameters.clientAuth().get().toUpperCase(Locale.ROOT)))
                .protocols(parameters.protocols())
                .ciphers(parameters.cipherSuites(), SupportedCipherSuiteFilter.INSTANCE)
                .sessionCacheSize(0)
                .sessionTimeout(0)
                .applicationProtocolConfig(
                    new ApplicationProtocolConfig(
                        ApplicationProtocolConfig.Protocol.ALPN,
                        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                        ApplicationProtocolNames.HTTP_2,
                        ApplicationProtocolNames.HTTP_1_1
                    )
                )
                .trustManager(parameters.trustManagerFactory().get())
                .build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    private static SslContext buildClientSslContext(SecureTransportSettingsProvider provider, Settings settings) {
        try {
            SecureTransportSettingsProvider.SecureTransportParameters parameters = provider.parameters(null).get();
            return SslContextBuilder.forClient()
                .sslProvider(SslProvider.valueOf(parameters.sslProvider().get().toUpperCase(Locale.ROOT)))
                .protocols(parameters.protocols())
                .ciphers(parameters.cipherSuites(), SupportedCipherSuiteFilter.INSTANCE)
                .applicationProtocolConfig(
                    new ApplicationProtocolConfig(
                        ApplicationProtocolConfig.Protocol.ALPN,
                        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                        ApplicationProtocolNames.HTTP_2,
                        ApplicationProtocolNames.HTTP_1_1
                    )
                )
                .sessionCacheSize(0)
                .sessionTimeout(0)
                .keyManager(parameters.keyManagerFactory().get())
                .trustManager(
                    NetworkModule.TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION.get(settings)
                        ? parameters.trustManagerFactory().get()
                        : InsecureTrustManagerFactory.INSTANCE
                )
                .build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }
}
