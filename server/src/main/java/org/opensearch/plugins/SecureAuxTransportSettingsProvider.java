/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.util.Optional;

/**
 * A provider for security related settings for gRPC transports.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureAuxTransportSettingsProvider {
    /**
     * Provides access to SSL params directly for cases where it is not convenient to consume a pre-built javax.net.ssl SSLContext.
     * @param settings settings
     * @return an instance of {@link SecureAuxTransportSettingsProvider.SecureTransportParameters}
     */
    Optional<SecureAuxTransportSettingsProvider.SecureTransportParameters> parameters(Settings settings);

    /**
     * Parameters for configuring secure transport connections.
     * Provides access to SSL/TLS configuration settings.
     */
    @ExperimentalApi
    interface SecureTransportParameters {
        /**
         * Determines if dual mode is enabled for handling both TLS and plaintext connections.
         * When enabled, the server can accept both secure and insecure connections on the same port.
         * @return true if dual mode is enabled, false otherwise
         */
        boolean dualModeEnabled();

        /**
         * Get the SSL provider implementation to use (e.g., "JDK", "OPENSSL").
         * @return the name of the SSL provider
         */
        String sslProvider();

        /**
         * Get the client authentication mode (e.g., "NONE", "OPTIONAL", "REQUIRE").
         * Determines whether client certificates are requested/required during handshake.
         * @return the client authentication setting
         */
        String clientAuth();

        /**
         * Get enabled TLS protocols (e.g., "TLSv1.2", "TLSv1.3").
         * @return the enabled protocols
         */
        Iterable<String> protocols();

        /**
         * Get enabled cipher suites for TLS connections.
         * @return the enabled cipher suites
         */
        Iterable<String> cipherSuites();

        /**
         * KeyManagerFactory which manages the server's identity credentials.
         * @return the key manager factory
         */
        KeyManagerFactory keyManagerFactory();

        /**
         * TrustManagerFactory which determines trusted client certificates.
         * @return the trust manager factory
         */
        TrustManagerFactory trustManagerFactory();
    }
}
