/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Default implementation of {@link SecureHttpTransportSettingsProvider.SecureHttpTransportParameters}.
 */
class DefaultSecureHttpTransportParameters implements SecureHttpTransportSettingsProvider.SecureHttpTransportParameters {
    @Override
    public Optional<KeyManagerFactory> keyManagerFactory() {
        return Optional.empty();
    }

    @Override
    public Optional<String> sslProvider() {
        return Optional.empty();
    }

    @Override
    public Optional<String> clientAuth() {
        return Optional.empty();
    }

    @Override
    public Collection<String> protocols() {
        return List.of();
    }

    @Override
    public Collection<String> cipherSuites() {
        return List.of();
    }

    @Override
    public Optional<TrustManagerFactory> trustManagerFactory() {
        return Optional.empty();
    }
}
