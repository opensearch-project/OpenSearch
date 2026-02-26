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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import java.util.List;
import java.util.Locale;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.AlpnAwareSSLEngineWrapper;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;

/**
 * Provides {@link SslContext} instances for Arrow Flight transport, backed by the live
 * {@link SSLContext} from {@link SecureTransportSettingsProvider}.
 * <p>
 * Both server and client wrap the live {@link SSLContext} in a {@link JdkSslContext}.
 * Because {@link JdkSslContext} holds a reference to the {@link SSLContext} and calls
 * {@code createSSLEngine()} on it per connection, re-initializing the underlying
 * {@link SSLContext} with new key material takes effect on the next handshake without
 * restarting the server or client.
 * <p>
 * The client uses {@link AlpnPresettingClientSslContext} rather than a bare {@link JdkSslContext}
 * because gRPC-netty's {@code ClientTlsHandler} unconditionally sets
 * {@code endpointIdentificationAlgorithm="HTTPS"} on every engine after {@code newEngine()}
 * returns. When hostname verification is disabled, this must be intercepted and stripped.
 * ALPN is also pre-set on the engine to guarantee it survives the {@code setSSLParameters}
 * round-trip performed by {@code ClientTlsHandler}.
 */
public class DefaultSslContextProvider implements SslContextProvider {

    private static final String[] DEFAULT_SSL_PROTOCOLS = { "TLSv1.3", "TLSv1.2", "TLSv1.1" };
    private static final String[] ALPN_PROTOCOLS = { ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1 };

    private static final ApplicationProtocolConfig ALPN_H2 = new ApplicationProtocolConfig(
        ApplicationProtocolConfig.Protocol.ALPN,
        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
        ALPN_PROTOCOLS
    );

    private final SecureTransportSettingsProvider secureTransportSettingsProvider;
    private final Settings settings;
    private final boolean enforceHostnameVerification;
    private volatile SslContext serverSslContext;
    private volatile SslContext clientSslContext;

    /**
     * Creates a new DefaultSslContextProvider.
     * @param secureTransportSettingsProvider provider for secure transport settings and live SSLContext
     * @param settings cluster settings
     */
    public DefaultSslContextProvider(SecureTransportSettingsProvider secureTransportSettingsProvider, Settings settings) {
        this.secureTransportSettingsProvider = secureTransportSettingsProvider;
        this.settings = settings;
        this.enforceHostnameVerification = NetworkModule.TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION.get(settings);
    }

    @Override
    public SslContext getServerSslContext() {
        if (serverSslContext == null) {
            synchronized (this) {
                if (serverSslContext == null) {
                    serverSslContext = buildJdkSslContext(false);
                }
            }
        }
        return serverSslContext;
    }

    @Override
    public SslContext getClientSslContext() {
        if (clientSslContext == null) {
            synchronized (this) {
                if (clientSslContext == null) {
                    clientSslContext = new AlpnPresettingClientSslContext(buildJdkSslContext(true), enforceHostnameVerification);
                }
            }
        }
        return clientSslContext;
    }

    private JdkSslContext buildJdkSslContext(boolean isClient) {
        try {
            SSLContext sslContext = secureTransportSettingsProvider.buildSecureTransportContext(settings)
                .orElseThrow(() -> new IllegalStateException("No SSLContext from SecureTransportSettingsProvider"));
            SecureTransportSettingsProvider.SecureTransportParameters params = secureTransportSettingsProvider.parameters(settings)
                .orElseThrow(() -> new IllegalStateException("No SecureTransportParameters from SecureTransportSettingsProvider"));
            ClientAuth clientAuth = ClientAuth.valueOf(params.clientAuth().orElse("NONE").toUpperCase(Locale.ROOT));
            return new JdkSslContext(
                sslContext,
                isClient,
                params.cipherSuites().isEmpty() ? null : params.cipherSuites(),
                SupportedCipherSuiteFilter.INSTANCE,
                ALPN_H2,
                isClient ? ClientAuth.NONE : clientAuth,
                DEFAULT_SSL_PROTOCOLS,
                enforceHostnameVerification
            );
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wraps a client {@link JdkSslContext} to intercept {@code newEngine()} and:
     * <ol>
     *   <li>Pre-set ALPN protocols on the engine so they survive gRPC-netty's
     *       {@code getSSLParameters()} / {@code setSSLParameters()} round-trip.</li>
     *   <li>When hostname verification is disabled, wrap the engine in
     *       {@link AlpnAwareSSLEngineWrapper} to strip the {@code endpointIdentificationAlgorithm}
     *       that {@code ClientTlsHandler} unconditionally sets after {@code newEngine()} returns.</li>
     * </ol>
     */
    private static final class AlpnPresettingClientSslContext extends SslContext {
        private final JdkSslContext delegate;
        private final boolean enforceHostnameVerification;

        AlpnPresettingClientSslContext(JdkSslContext delegate, boolean enforceHostnameVerification) {
            this.delegate = delegate;
            this.enforceHostnameVerification = enforceHostnameVerification;
        }

        @Override
        public boolean isClient() {
            return true;
        }

        @Override
        public List<String> cipherSuites() {
            return delegate.cipherSuites();
        }

        @Override
        public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
            return delegate.applicationProtocolNegotiator();
        }

        @Override
        public javax.net.ssl.SSLSessionContext sessionContext() {
            return delegate.sessionContext();
        }

        @Override
        public SSLEngine newEngine(ByteBufAllocator alloc) {
            return wrap(delegate.newEngine(alloc));
        }

        @Override
        public SSLEngine newEngine(ByteBufAllocator alloc, String peerHost, int peerPort) {
            return wrap(delegate.newEngine(alloc, peerHost, peerPort));
        }

        private SSLEngine wrap(SSLEngine engine) {
            // Pre-set ALPN so it is present when ClientTlsHandler reads getSSLParameters()
            // before adding endpointIdentificationAlgorithm and calling setSSLParameters().
            SSLParameters params = engine.getSSLParameters();
            params.setApplicationProtocols(ALPN_PROTOCOLS);
            engine.setSSLParameters(params);
            if (enforceHostnameVerification) {
                return engine;
            }
            // AlpnAwareSSLEngineWrapper strips endpointIdentificationAlgorithm on every
            // setSSLParameters() call and implements ApplicationProtocolAccessor (package-private
            // in Netty) so SslHandler.applicationProtocol() can read the negotiated protocol.
            // It must live in the io.netty.handler.ssl package to access that interface.
            return new AlpnAwareSSLEngineWrapper(engine);
        }
    }
}
