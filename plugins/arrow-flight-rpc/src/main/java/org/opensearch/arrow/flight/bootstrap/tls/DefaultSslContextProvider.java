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
 * DefaultSslContextProvider is an implementation of the SslContextProvider interface that provides SSL contexts based on the provided SecureTransportSettingsProvider.
 */
public class DefaultSslContextProvider implements SslContextProvider {

    private final SecureTransportSettingsProvider secureTransportSettingsProvider;
    private final Settings settings;

    /**
     * Constructor for DefaultSslContextProvider.
     * @param secureTransportSettingsProvider The SecureTransportSettingsProvider instance.
     * @param settings The cluster settings.
     */
    public DefaultSslContextProvider(SecureTransportSettingsProvider secureTransportSettingsProvider, Settings settings) {
        this.secureTransportSettingsProvider = secureTransportSettingsProvider;
        this.settings = settings;
    }

    // TODO - handle certificates reload
    /**
     * Creates and returns the server SSL context based on the provided SecureTransportSettingsProvider.
     * @return The server SSL context.
     */
    @Override
    public SslContext getServerSslContext() {
        try {
            SecureTransportSettingsProvider.SecureTransportParameters parameters = secureTransportSettingsProvider.parameters(null).get();
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
                        // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
                        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                        // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
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

    /**
    * Returns the client SSL context based on the provided SecureTransportSettingsProvider.
    * @return The client SSL context.
    */
    @Override
    public SslContext getClientSslContext() {
        try {
            SecureTransportSettingsProvider.SecureTransportParameters parameters = secureTransportSettingsProvider.parameters(null).get();

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
