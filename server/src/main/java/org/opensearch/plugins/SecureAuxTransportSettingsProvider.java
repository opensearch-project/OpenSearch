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
     * @return an instance of SSLContext.
     */
    default Optional<SSLContext> buildSecureAuxServerTransportContext(Settings settings, NetworkPlugin.AuxTransport transport)
        throws SSLException {
        return Optional.empty();
    }

    /**
     * Additional params required for configuring ALPN.
     * @return an instance of {@link SecureAuxTransportSettingsProvider.SecureAuxTransportParameters}
     */
    default Optional<SecureAuxTransportSettingsProvider.SecureAuxTransportParameters> parameters() {
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
