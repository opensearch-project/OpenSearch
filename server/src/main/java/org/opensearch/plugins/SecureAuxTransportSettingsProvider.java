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
import org.opensearch.transport.AuxTransport;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import java.util.Collection;
import java.util.Optional;

/**
 * A security settings provider for auxiliary transports.
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureAuxTransportSettingsProvider {
    /**
     * Fetch an SSLContext as managed by pluggable security provider.
     * @param settings for providing additional configuration options when building the ssl context.
     * @param transport the auxiliary transport for which an SSLContext is built.
     * @return an instance of SSLContext.
     */
    default Optional<SSLContext> buildSecureAuxServerTransportContext(Settings settings, AuxTransport transport) throws SSLException {
        return Optional.empty();
    }

    /**
     * Additional params required for configuring ALPN.
     * @param transport the auxiliary transport to be provided SecureAuxTransportParameters.
     * @return an instance of {@link SecureAuxTransportSettingsProvider.SecureAuxTransportParameters}
     */
    default Optional<SecureAuxTransportSettingsProvider.SecureAuxTransportParameters> parameters(AuxTransport transport) {
        return Optional.empty();
    }

    /**
     * ALPN configuration parameters.
     */
    @ExperimentalApi
    interface SecureAuxTransportParameters {
        Optional<String> clientAuth();

        Collection<String> cipherSuites();
    }
}
