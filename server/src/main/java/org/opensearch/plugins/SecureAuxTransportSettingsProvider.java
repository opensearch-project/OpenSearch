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
     * @param auxTransportType key for enabling this transport with AUX_TRANSPORT_TYPES_SETTING.
     * @return an instance of SSLContext.
     */
    default Optional<SSLContext> buildSecureAuxServerTransportContext(Settings settings, String auxTransportType) throws SSLException {
        return Optional.empty();
    }

    /**
     * Additional params required for configuring ALPN.
     * @param settings for providing additional configuration options when building secure params.
     * @param auxTransportType key for enabling this transport with AUX_TRANSPORT_TYPES_SETTING.
     * @return an instance of {@link SecureAuxTransportSettingsProvider.SecureAuxTransportParameters}
     */
    default Optional<SecureAuxTransportSettingsProvider.SecureAuxTransportParameters> parameters(Settings settings, String auxTransportType)
        throws SSLException {
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
