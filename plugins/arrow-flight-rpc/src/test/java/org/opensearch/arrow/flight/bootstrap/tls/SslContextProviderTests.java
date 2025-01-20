/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.bootstrap.tls;

import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SslContextProviderTests extends OpenSearchTestCase {

    private SecureTransportSettingsProvider mockSecureTransportSettingsProvider;
    private SecureTransportSettingsProvider.SecureTransportParameters mockParameters;
    private KeyManagerFactory keyManagerFactory;
    private TrustManagerFactory trustManagerFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        mockSecureTransportSettingsProvider = mock(SecureTransportSettingsProvider.class);
        mockParameters = mock(SecureTransportSettingsProvider.SecureTransportParameters.class);

        Iterable<String> protocols = Arrays.asList("TLSv1.2", "TLSv1.3");
        Iterable<String> cipherSuites = Arrays.asList("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        setupDummyFactories();
        when(mockParameters.sslProvider()).thenReturn("JDK");
        when(mockParameters.clientAuth()).thenReturn("REQUIRE");
        when(mockParameters.protocols()).thenReturn(protocols);
        when(mockParameters.cipherSuites()).thenReturn(cipherSuites);
        when(mockParameters.keyManagerFactory()).thenReturn(keyManagerFactory);
        when(mockParameters.trustManagerFactory()).thenReturn(trustManagerFactory);

        when(mockSecureTransportSettingsProvider.parameters(null)).thenReturn(java.util.Optional.of(mockParameters));
    }

    private void setupDummyFactories() throws KeyStoreException, CertificateException, IOException, NoSuchAlgorithmException,
        UnrecoverableKeyException {
        KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        keystore.load(null, new char[0]);

        keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keystore, new char[0]);

        trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keystore);
    }

    public void testEnabledSslContextProvider() {
        SslContextProvider provider = new DefaultSslContextProvider(mockSecureTransportSettingsProvider);

        assertTrue("SSL should be enabled", provider.isSslEnabled());
        assertNotNull(provider.getServerSslContext());
        assertNotNull(provider.getClientSslContext());
    }

    public void testDisabledSslContextProvider() {
        SslContextProvider provider = new DisabledSslContextProvider();

        assertFalse("SSL should be disabled", provider.isSslEnabled());
        assertNull("Server SSL context should be null", provider.getServerSslContext());
        assertNull("Client SSL context should be null", provider.getClientSslContext());
    }

    public void testDefaultSslContextProviderWithNullSupplier() {
        expectThrows(NullPointerException.class, () -> {
            SslContextProvider sslContextProvider = new DefaultSslContextProvider(null);
            sslContextProvider.getServerSslContext();
        });
    }

    public void testDefaultSslContextProviderWithInvalidSslProvider() {
        when(mockParameters.sslProvider()).thenReturn("INVALID");
        DefaultSslContextProvider provider = new DefaultSslContextProvider(mockSecureTransportSettingsProvider);
        expectThrows(IllegalArgumentException.class, provider::getServerSslContext);
        expectThrows(IllegalArgumentException.class, provider::getClientSslContext);
    }
}
