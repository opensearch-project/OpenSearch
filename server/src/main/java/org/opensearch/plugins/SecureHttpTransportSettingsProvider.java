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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

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
     * Collection of additional {@link TransportAdapterProvider}s that are specific to particular HTTP transport
     * @param settings settings
     * @return a collection of additional {@link TransportAdapterProvider}s
     */
    default Collection<TransportAdapterProvider<HttpServerTransport>> getHttpTransportAdapterProviders(Settings settings) {
        return Collections.emptyList();
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
