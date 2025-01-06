/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * Default implementation of {@link SecureTransportSettingsProvider.SecureTransportParameters}.
 */
class DefaultSecureTransportParameters implements SecureTransportSettingsProvider.SecureTransportParameters {
    private final Settings settings;

    DefaultSecureTransportParameters(Settings settings) {
        this.settings = settings;
    }

    @Override
    public boolean dualModeEnabled() {
        return NetworkModule.TRANSPORT_SSL_DUAL_MODE_ENABLED.get(settings);
    }

    @Override
    public KeyManagerFactory keyManagerFactory() {
        return null;
    }

    @Override
    public String sslProvider() {
        return "";
    }

    @Override
    public String clientAuth() {
        return "";
    }

    @Override
    public Iterable<String> protocols() {
        return null;
    }

    @Override
    public Iterable<String> cipherSuites() {
        return null;
    }

    @Override
    public TrustManagerFactory trustManagerFactory() {
        return null;
    }
}
