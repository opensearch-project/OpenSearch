/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import static org.opensearch.test.OpenSearchTestCase.randomFrom;

public class SecureSettingsHelpers {
    private static final String TEST_PASS = "password"; // used for all keystores
    static final String SERVER_KEYSTORE = "/netty4-server-secure.jks";
    static final String CLIENT_KEYSTORE = "/netty4-client-secure.jks";
    static final String[] DEFAULT_SSL_PROTOCOLS = { "TLSv1.3", "TLSv1.2", "TLSv1.1" };
    static final String[] DEFAULT_CIPHERS = {
        "TLS_AES_128_GCM_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" };

    /**
     * Exception messages for various types of TLS client/server connection failure.
     * We would like to check to ensure a connection fails in the way we expect.
     * However, depending on the default JDK provider exceptions may differ slightly,
     * so we allow a couple different error messages for each possible error.
     */
    public enum ConnectExceptions {
        NONE(List.of("Connection succeeded")),
        UNAVAILABLE(List.of("Network closed for unknown reason")),
        BAD_CERT(List.of("bad_certificate", "certificate_required"));

        List<String> msgList = null;

        ConnectExceptions(List<String> exceptionMsg) {
            this.msgList = exceptionMsg;
        }

        public static ConnectExceptions get(Throwable e) {
            if (e.getMessage() != null) {
                for (ConnectExceptions exception : values()) {
                    if (exception == NONE) continue; // Skip success message
                    if (exception.msgList.stream().anyMatch(substring -> e.getMessage().contains(substring))) {
                        return exception;
                    }
                }
            }
            if (e.getCause() != null) {
                return get(e.getCause());
            }
            throw new RuntimeException("Unexpected exception", e);
        }
    }

    public static KeyManagerFactory getTestKeyManagerFactory(String keystorePath) {
        KeyManagerFactory keyManagerFactory;
        try {
            final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(SecureNetty4GrpcServerTransport.class.getResourceAsStream(keystorePath), TEST_PASS.toCharArray());
            keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, TEST_PASS.toCharArray());
        } catch (UnrecoverableKeyException | CertificateException | KeyStoreException | IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return keyManagerFactory;
    }

    static TrustManagerFactory getTestTrustManagerFactory(String keystorePath) {
        try {
            final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(SecureNetty4GrpcServerTransport.class.getResourceAsStream(keystorePath), TEST_PASS.toCharArray());
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            return trustManagerFactory;
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    static SecureAuxTransportSettingsProvider getSecureSettingsProvider(
        String clientAuth,
        KeyManagerFactory keyMngerFactory,
        TrustManagerFactory trustMngerFactory
    ) {
        return new SecureAuxTransportSettingsProvider() {
            @Override
            public Optional<SSLContext> buildSecureAuxServerTransportContext(Settings settings, String auxTransportType)
                throws SSLException {
                // Choose a random protocol from among supported test defaults
                String protocol = randomFrom(DEFAULT_SSL_PROTOCOLS);
                // Default JDK provider
                SSLContext testContext;
                try {
                    testContext = SSLContext.getInstance(protocol);
                    testContext.init(keyMngerFactory.getKeyManagers(), trustMngerFactory.getTrustManagers(), new SecureRandom());
                } catch (NoSuchAlgorithmException | KeyManagementException e) {
                    throw new SSLException("Failed to build mock provider", e);
                }
                return Optional.of(testContext);
            }

            @Override
            public Optional<SecureAuxTransportParameters> parameters(Settings settings, String auxTransportType) {
                return Optional.of(new SecureAuxTransportParameters() {
                    @Override
                    public Optional<String> clientAuth() {
                        return Optional.of(clientAuth);
                    }

                    @Override
                    public Collection<String> cipherSuites() {
                        return List.of(DEFAULT_CIPHERS);
                    }
                });
            }
        };
    }

    public static SecureAuxTransportSettingsProvider getServerClientAuthRequired() {
        return getSecureSettingsProvider(
            ClientAuth.REQUIRE.name().toUpperCase(Locale.ROOT),
            getTestKeyManagerFactory(SERVER_KEYSTORE),
            getTestTrustManagerFactory(CLIENT_KEYSTORE)
        );
    }

    public static SecureAuxTransportSettingsProvider getServerClientAuthOptional() {
        return getSecureSettingsProvider(
            ClientAuth.OPTIONAL.name().toUpperCase(Locale.ROOT),
            getTestKeyManagerFactory(SERVER_KEYSTORE),
            getTestTrustManagerFactory(CLIENT_KEYSTORE)
        );
    }

    public static SecureAuxTransportSettingsProvider getServerClientAuthNone() {
        return getSecureSettingsProvider(
            ClientAuth.NONE.name().toUpperCase(Locale.ROOT),
            getTestKeyManagerFactory(SERVER_KEYSTORE),
            InsecureTrustManagerFactory.INSTANCE
        );
    }
}
