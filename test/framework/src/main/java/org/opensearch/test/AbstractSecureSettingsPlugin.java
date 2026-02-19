/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.test;

import org.opensearch.common.settings.Settings;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider;
import org.opensearch.plugins.SecureSettingsFactory;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.plugins.TransportExceptionHandler;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import io.netty.pkitesting.CertificateBuilder.Algorithm;

import static org.opensearch.test.KeyStoreUtils.KEYSTORE_PASSWORD;

public abstract class AbstractSecureSettingsPlugin extends Plugin {
    private final Collection<String> cipherSuites;
    private final TrustManagerFactory trustManagerFactory;

    public AbstractSecureSettingsPlugin(TrustManagerFactory trustManagerFactory, Collection<String> cipherSuites) {
        this.cipherSuites = cipherSuites;
        this.trustManagerFactory = trustManagerFactory;
    }

    @Override
    public Optional<SecureSettingsFactory> getSecureSettingFactory(Settings settings) {
        try {
            final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("PKIX");
            keyManagerFactory.init(KeyStoreUtils.createServerKeyStore(Algorithm.ecp384), KEYSTORE_PASSWORD);

            return Optional.of(new SecureSettingsFactory() {
                @Override
                public Optional<SecureTransportSettingsProvider> getSecureTransportSettingsProvider(Settings settings) {
                    return Optional.empty();
                }

                @Override
                public Optional<SecureHttpTransportSettingsProvider> getSecureHttpTransportSettingsProvider(Settings settings) {
                    return Optional.of(new SecureHttpTransportSettingsProvider() {
                        @Override
                        public Optional<SecureHttpTransportParameters> parameters(Settings settings) {
                            return Optional.of(new SecureHttpTransportParameters() {
                                @Override
                                public Optional<KeyManagerFactory> keyManagerFactory() {
                                    return Optional.of(keyManagerFactory);
                                }

                                @Override
                                public Optional<String> sslProvider() {
                                    return Optional.empty();
                                }

                                @Override
                                public Optional<String> clientAuth() {
                                    return Optional.empty();
                                }

                                @Override
                                public Collection<String> protocols() {
                                    return List.of("TLSv1.3", "TLSv1.2", "TLSv1.1");
                                }

                                @Override
                                public Collection<String> cipherSuites() {
                                    return cipherSuites;
                                }

                                @Override
                                public Optional<TrustManagerFactory> trustManagerFactory() {
                                    return Optional.of(trustManagerFactory);
                                }
                            });
                        }

                        @Override
                        public Optional<TransportExceptionHandler> buildHttpServerExceptionHandler(
                            Settings settings,
                            HttpServerTransport transport
                        ) {
                            return Optional.empty();
                        }

                        @Override
                        public Optional<SSLEngine> buildSecureHttpServerEngine(Settings settings, HttpServerTransport transport)
                            throws SSLException {
                            return Optional.of(newSSLEngine(keyManagerFactory));
                        }
                    });
                }

                @Override
                public Optional<SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(Settings settings) {
                    return Optional.empty();
                }
            });
        } catch (RuntimeException | Error ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Creates new {@link SSLEngine} instance
     * @param keyManagerFactory key manager factory
     * @return new {@link SSLEngine} instance
     */
    protected abstract SSLEngine newSSLEngine(KeyManagerFactory keyManagerFactory) throws SSLException;
}
