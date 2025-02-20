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

    @ExperimentalApi
    interface SecureTransportParameters {
        boolean dualModeEnabled();

        String sslProvider();

        String clientAuth();

        Iterable<String> protocols();

        Iterable<String> cipherSuites();

        KeyManagerFactory keyManagerFactory();

        TrustManagerFactory trustManagerFactory();
    }
}
