/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import org.opensearch.OpenSearchException;
import org.opensearch.plugins.SecureTransportSettingsProvider;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;

/**
 * DefaultSslContextProvider is an implementation of the SslContextProvider interface that provides SSL contexts based on the provided SecureTransportSettingsProvider.
 */
public class DefaultSslContextProvider implements SslContextProvider {

    private final SecureTransportSettingsProvider secureTransportSettingsProvider;

    /**
     * Constructor for DefaultSslContextProvider.
     * @param secureTransportSettingsProvider The SecureTransportSettingsProvider instance.
     */
    public DefaultSslContextProvider(SecureTransportSettingsProvider secureTransportSettingsProvider) {
        this.secureTransportSettingsProvider = secureTransportSettingsProvider;
    }

    /**
     * Returns true to indicate that SSL is enabled.
     * @return true
     */
    @Override
    public boolean isSslEnabled() {
        return true;
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
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<SslContext>) () -> io.netty.handler.ssl.SslContextBuilder.forServer(
                    parameters.keyManagerFactory()
                )
                    .sslProvider(SslProvider.valueOf(parameters.sslProvider().toUpperCase(Locale.ROOT)))
                    .clientAuth(ClientAuth.valueOf(parameters.clientAuth().toUpperCase(Locale.ROOT)))
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
                    .trustManager(parameters.trustManagerFactory())
                    .build()
            );
        } catch (PrivilegedActionException e) {
            throw new OpenSearchException("Failed to build server SSL context", e);
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
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<SslContext>) () -> io.netty.handler.ssl.SslContextBuilder.forClient()
                    .sslProvider(SslProvider.valueOf(parameters.sslProvider().toUpperCase(Locale.ROOT)))
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
                    .keyManager(parameters.keyManagerFactory())
                    .trustManager(parameters.trustManagerFactory())
                    .build()
            );
        } catch (PrivilegedActionException e) {
            throw new OpenSearchException("Failed to build client SSL context", e);
        }
    }
}
