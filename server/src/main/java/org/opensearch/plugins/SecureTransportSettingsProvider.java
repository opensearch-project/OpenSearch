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
import org.opensearch.transport.TcpTransport;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.util.Optional;

/**
 * A provider for security related settings for transports.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureTransportSettingsProvider {
    /**
     * An exception handler for errors that might happen while secure transport handle the requests.
     *
     * @see <a href="https://github.com/opensearch-project/security/blob/main/src/main/java/org/opensearch/security/ssl/SslExceptionHandler.java">SslExceptionHandler</a>
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    @FunctionalInterface
    interface ServerExceptionHandler {
        static ServerExceptionHandler NOOP = t -> {};

        /**
         * Handler for errors happening during the server side processing of the requests
         * @param t the error
         */
        void onError(Throwable t);
    }

    /**
     * If supported, builds the {@link ServerExceptionHandler} instance for {@link HttpServerTransport} instance
     * @param settings settings
     * @param transport {@link HttpServerTransport} instance
     * @return if supported, builds the {@link ServerExceptionHandler} instance
     */
    Optional<ServerExceptionHandler> buildHttpServerExceptionHandler(Settings settings, HttpServerTransport transport);

    /**
     * If supported, builds the {@link ServerExceptionHandler} instance for {@link TcpTransport} instance
     * @param settings settings
     * @param transport {@link TcpTransport} instance
     * @return if supported, builds the {@link ServerExceptionHandler} instance
     */
    Optional<ServerExceptionHandler> buildServerTransportExceptionHandler(Settings settings, TcpTransport transport);

    /**
     * If supported, builds the {@link SSLEngine} instance for {@link HttpServerTransport} instance
     * @param settings settings
     * @param transport {@link HttpServerTransport} instance
     * @return if supported, builds the {@link SSLEngine} instance
     * @throws SSLException throws SSLException if the {@link SSLEngine} instance cannot be built
     */
    Optional<SSLEngine> buildSecureHttpServerEngine(Settings settings, HttpServerTransport transport) throws SSLException;

    /**
     * If supported, builds the {@link SSLEngine} instance for {@link TcpTransport} instance
     * @param settings settings
     * @param transport {@link TcpTransport} instance
     * @return if supported, builds the {@link SSLEngine} instance
     * @throws SSLException throws SSLException if the {@link SSLEngine} instance cannot be built
     */
    Optional<SSLEngine> buildSecureServerTransportEngine(Settings settings, TcpTransport transport) throws SSLException;

    /**
     * If supported, builds the {@link SSLEngine} instance for client transport instance
     * @param settings settings
     * @param hostname host name
     * @param port port
     * @return if supported, builds the {@link SSLEngine} instance
     * @throws SSLException throws SSLException if the {@link SSLEngine} instance cannot be built
     */
    Optional<SSLEngine> buildSecureClientTransportEngine(Settings settings, String hostname, int port) throws SSLException;
}
