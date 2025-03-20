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
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.util.Collection;
import java.util.Optional;

/**
 * A security settings provider for auxiliary transports.
 * As auxiliary transports are pluggable security params are provided in a generic way for transports
 * to construct a ssl context for their particular transport implementation.
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureAuxTransportSettingsProvider {
    /**
     * Parameters that can be provided by the {@link SecureAuxTransportSettingsProvider}.
     * Includes all fields required to build a ssl context.
     * Note that these fields are dynamic.
     * A new {@link SecureAuxTransportSettingsProvider.SecureAuxTransportParameters} may be returned on each call.
     * @return an instance of {@link SecureAuxTransportSettingsProvider.SecureAuxTransportParameters}
     */
    default Optional<SecureAuxTransportSettingsProvider.SecureAuxTransportParameters> parameters() {
        return Optional.empty();
    }

    /**
     * Contains params required to construct a generic ssl context.
     */
    @ExperimentalApi
    interface SecureAuxTransportParameters {
        boolean dualModeEnabled();

        Optional<KeyManagerFactory> keyManagerFactory();

        Optional<String> sslProvider();

        Optional<String> clientAuth();

        Collection<String> protocols();

        Collection<String> cipherSuites();

        Optional<TrustManagerFactory> trustManagerFactory();
    }
}
