/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Transport;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class SecureTransportSettingsProviderTests extends OpenSearchTestCase {

    public void testParameterDefaults() throws Exception {
        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        final SecureTransportSettingsProvider.SecureTransportParameters parameters = parameters(keyManagerFactory, trustManagerFactory);

        assertSame(keyManagerFactory, parameters.clientKeyManagerFactory().orElseThrow());
        assertSame(trustManagerFactory, parameters.clientTrustManagerFactory().orElseThrow());
        assertEquals(Optional.empty(), parameters.enforceHostnameVerification());
        assertEquals(Optional.empty(), parameters.resolveHostname());
    }

    public void testProviderDefaults() {
        final SecureTransportSettingsProvider provider = new SecureTransportSettingsProvider() {
            @Override
            public Optional<TransportExceptionHandler> buildServerTransportExceptionHandler(Settings settings, Transport transport) {
                return Optional.empty();
            }

            @Override
            public Optional<SSLEngine> buildSecureServerTransportEngine(Settings settings, Transport transport) {
                return Optional.empty();
            }

            @Override
            public Optional<SSLEngine> buildSecureClientTransportEngine(Settings settings, String hostname, int port) {
                return Optional.empty();
            }
        };

        provider.addReloadListener(() -> fail("a provider without reload support must not call the listener"));
        assertEquals(Optional.empty(), provider.isNodeCertificate(new X509Certificate[0]));
    }

    private static SecureTransportSettingsProvider.SecureTransportParameters parameters(
        final KeyManagerFactory keyManagerFactory,
        final TrustManagerFactory trustManagerFactory
    ) {
        return new SecureTransportSettingsProvider.SecureTransportParameters() {
            @Override
            public boolean dualModeEnabled() {
                return false;
            }

            @Override
            public Optional<KeyManagerFactory> keyManagerFactory() {
                return Optional.of(keyManagerFactory);
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
                return Optional.of(trustManagerFactory);
            }
        };
    }
}
