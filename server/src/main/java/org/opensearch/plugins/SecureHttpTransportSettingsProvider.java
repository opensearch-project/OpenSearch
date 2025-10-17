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
import org.opensearch.http.HttpServerTransport;
import org.opensearch.transport.TransportAdapterProvider;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * A provider for security related settings for HTTP transports.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureHttpTransportSettingsProvider {
    /**
     * The well-known name of header verifier {@link TransportAdapterProvider} provider instance
     */
    final String REQUEST_HEADER_VERIFIER = "HeaderVerifier";

    /**
     * The well-known name of request decompressor {@link TransportAdapterProvider} provider instance
     */
    final String REQUEST_DECOMPRESSOR = "RequestDecompressor";

    /**
     * Dynamic parameters that can be provided by the {@link SecureHttpTransportParameters}
     */
    @ExperimentalApi
    interface SecureHttpTransportParameters {
        /**
         * Provides the instance of {@link KeyManagerFactory}
         * @return instance of {@link KeyManagerFactory}
         */
        Optional<KeyManagerFactory> keyManagerFactory();

        /**
         * Provides the SSL provider (JDK, OpenSsl, ...) if supported by transport
         * @return SSL provider
         */
        Optional<String> sslProvider();

        /**
         * Provides desired client authentication level
         * @return client authentication level
         */
        Optional<String> clientAuth();

        /**
         * Provides the list of supported protocols
         * @return list of supported protocols
         */
        Collection<String> protocols();

        /**
         * Provides the list of supported cipher suites
         * @return list of supported cipher suites
         */
        Collection<String> cipherSuites();

        /**
         * Provides the instance of {@link TrustManagerFactory}
         * @return instance of {@link TrustManagerFactory}
         */
        Optional<TrustManagerFactory> trustManagerFactory();
    }

    /**
     * Collection of additional {@link TransportAdapterProvider}s that are specific to particular HTTP transport
     * @param settings settings
     * @return a collection of additional {@link TransportAdapterProvider}s
     */
    default Collection<TransportAdapterProvider<HttpServerTransport>> getHttpTransportAdapterProviders(Settings settings) {
        return Collections.emptyList();
    }

    /**
     * Returns parameters that can be dynamically provided by a plugin providing a {@link SecureHttpTransportParameters}
     * implementation
     * @param settings settings
     * @return an instance of {@link SecureHttpTransportParameters}
     */
    default Optional<SecureHttpTransportParameters> parameters(Settings settings) {
        return Optional.of(new DefaultSecureHttpTransportParameters());
    }

    /**
     * If supported, builds the {@link TransportExceptionHandler} instance for {@link HttpServerTransport} instance
     * @param settings settings
     * @param transport {@link HttpServerTransport} instance
     * @return if supported, builds the {@link TransportExceptionHandler} instance
     */
    Optional<TransportExceptionHandler> buildHttpServerExceptionHandler(Settings settings, HttpServerTransport transport);

    /**
     * If supported, builds the {@link SSLEngine} instance for {@link HttpServerTransport} instance
     * @param settings settings
     * @param transport {@link HttpServerTransport} instance
     * @return if supported, builds the {@link SSLEngine} instance
     * @throws SSLException throws SSLException if the {@link SSLEngine} instance cannot be built
     */
    Optional<SSLEngine> buildSecureHttpServerEngine(Settings settings, HttpServerTransport transport) throws SSLException;
}
