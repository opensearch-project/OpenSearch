/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.util.Collection;
import java.util.Optional;

/**
 * A security setting provider for auxiliary transports.
 * As auxiliary transports are pluggable, SSLContextBuilder is provided as a generic way for transports
 * to construct a ssl context for their particular transport implementation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureAuxTransportSettingsProvider {
    /**
     * Parameters needed for constructing a generic ssl context implementation.
     */
    @ExperimentalApi
    class SSLContextBuilder {
        protected boolean dualModeEnabled = false;
        protected String sslProvider;
        protected String clientAuth;
        protected Collection<String> protocols;
        protected Collection<String> cipherSuites;
        protected KeyManagerFactory keyManagerFactory;
        protected TrustManagerFactory trustManagerFactory;

        public SSLContextBuilder() {}

        /**
         * Determines if dual mode is enabled for handling both TLS and plaintext connections.
         * When enabled, the server can accept both secure and insecure connections on the same port.
         * @return true if dual mode is enabled, false otherwise
         */
        public boolean isDualModeEnabled() {
            return dualModeEnabled;
        }

        public SSLContextBuilder setDualModeEnabled(boolean enabled) {
            this.dualModeEnabled = enabled;
            return this;
        }

        /**
         * Get the SSL provider implementation to use (e.g., "JDK", "OPENSSL").
         * @return the name of the SSL provider
         */
        public Optional<String> getSslProvider() {
            return Optional.of(sslProvider);
        }

        public SSLContextBuilder setSslProvider(String sslProvider) {
            this.sslProvider = sslProvider;
            return this;
        }

        /**
         * Get the client authentication mode (e.g., "NONE", "OPTIONAL", "REQUIRE").
         * Determines whether client certificates are requested/required during handshake.
         * @return the client authentication setting
         */
        public Optional<String> getClientAuth() {
            return Optional.of(clientAuth);
        }

        public SSLContextBuilder setClientAuth(String clientAuth) {
            this.clientAuth = clientAuth;
            return this;
        }

        /**
         * Get enabled TLS protocols (e.g., "TLSv1.2", "TLSv1.3").
         * @return the enabled protocols
         */
        public Collection<String> getProtocols() {
            return protocols;
        }

        public SSLContextBuilder setProtocols(Collection<String> protocols) {
            this.protocols = protocols;
            return this;
        }

        /**
         * Get enabled cipher suites for TLS connections.
         * @return the enabled cipher suites
         */
        public Collection<String> getCipherSuites() {
            return cipherSuites;
        }

        public SSLContextBuilder setCipherSuites(Collection<String> cipherSuites) {
            this.cipherSuites = cipherSuites;
            return this;
        }

        /**
         * KeyManagerFactory which manages the server's identity credentials.
         * @return the key manager factory
         */
        public Optional<KeyManagerFactory> getKeyManagerFactory() {
            return Optional.of(keyManagerFactory);
        }

        public SSLContextBuilder setKeyManagerFactory(KeyManagerFactory keyManagerFactory) {
            this.keyManagerFactory = keyManagerFactory;
            return this;
        }

        /**
         * TrustManagerFactory which determines trusted client certificates.
         * @return the trust manager factory
         */
        public Optional<TrustManagerFactory> getTrustManagerFactory() {
            return Optional.of(trustManagerFactory);
        }

        public SSLContextBuilder setTrustManagerFactory(TrustManagerFactory trustManagerFactory) {
            this.trustManagerFactory = trustManagerFactory;
            return this;
        }
    }

    /**
     * Provides access to SSL params through a builder.
     * Aux transports are pluggable and will not all consume the same ssl context implementation.
     * @return an instance of {@link SecureAuxTransportSettingsProvider.SSLContextBuilder}
     */
    Optional<SecureAuxTransportSettingsProvider.SSLContextBuilder> getSSLContextBuilder();
}
