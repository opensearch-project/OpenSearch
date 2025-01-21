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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * A provider for security related settings for gRPC transports.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureAuxTransportSettingsProvider {
    /**
     * If supported, builds an {@link SSLContext} instance for {@link NetworkPlugin.AuxTransport} instance
     * @param settings settings
     * @param transport {@link NetworkPlugin.AuxTransport} instance
     * @return if supported, builds the {@link SSLContext} instance
     * @throws SSLException throws SSLException if the {@link SSLEngine} instance cannot be built
     */
    Optional<SSLContext> buildSecureAuxServerSSLContext(Settings settings, NetworkPlugin.AuxTransport transport) throws SSLException;
}
