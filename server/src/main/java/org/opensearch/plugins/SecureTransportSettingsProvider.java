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
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportAdapterProvider;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * A provider for security related settings for transports.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureTransportSettingsProvider {
    /**
     * Collection of additional {@link TransportAdapterProvider}s that are specific to particular transport
     * @param settings settings
     * @return a collection of additional {@link TransportAdapterProvider}s
     */
    default Collection<TransportAdapterProvider<Transport>> getTransportAdapterProviders(Settings settings) {
        return Collections.emptyList();
    }

    /**
     * If supported, builds the {@link TransportExceptionHandler} instance for {@link Transport} instance
     * @param settings settings
     * @param transport {@link Transport} instance
     * @return if supported, builds the {@link TransportExceptionHandler} instance
     */
    Optional<TransportExceptionHandler> buildServerTransportExceptionHandler(Settings settings, Transport transport);

    /**
     * If supported, builds the {@link SSLEngine} instance for {@link Transport} instance
     * @param settings settings
     * @param transport {@link Transport} instance
     * @return if supported, builds the {@link SSLEngine} instance
     * @throws SSLException throws SSLException if the {@link SSLEngine} instance cannot be built
     */
    Optional<SSLEngine> buildSecureServerTransportEngine(Settings settings, Transport transport) throws SSLException;

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
