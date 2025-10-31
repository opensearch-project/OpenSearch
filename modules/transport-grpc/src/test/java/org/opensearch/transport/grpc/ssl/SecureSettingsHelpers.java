/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import org.opensearch.common.Randomness;
import org.opensearch.common.settings.Settings;
import org.opensearch.fips.FipsMode;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class SecureSettingsHelpers {
    private static final String keyStoreType = FipsMode.CHECK.isFipsEnabled() ? "BCFKS" : "JKS";
    private static final String fileExtension = FipsMode.CHECK.isFipsEnabled() ? ".bcfks" : ".jks";
    private static final String provider = FipsMode.CHECK.isFipsEnabled() ? "BCJSSE" : "SunJSSE";
    private static final char[] TEST_PASS = "password".toCharArray(); // used for all keystores
    static final String SERVER_KEYSTORE = "/netty4-server-secure";
    static final String CLIENT_KEYSTORE = "/netty4-client-secure";

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
        try (InputStream keyStoreFile = SecureNetty4GrpcServerTransport.class.getResourceAsStream(keystorePath + fileExtension)) {
            KeyStore keyStore = KeyStore.getInstance(keyStoreType);
            keyStore.load(keyStoreFile, TEST_PASS);
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, TEST_PASS);
            return keyManagerFactory;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static TrustManagerFactory getTestTrustManagerFactory(String keystorePath) {
        try (InputStream trustStoreFile = SecureNetty4GrpcServerTransport.class.getResourceAsStream(keystorePath + fileExtension);) {
            final KeyStore trustStore = KeyStore.getInstance(keyStoreType);
            trustStore.load(trustStoreFile, TEST_PASS);
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            return trustManagerFactory;
        } catch (Exception e) {
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
                // Default JDK provider
                SSLContext testContext;
                try {
                    testContext = SSLContext.getInstance("TLS", provider);
                    testContext.init(keyMngerFactory.getKeyManagers(), trustMngerFactory.getTrustManagers(), Randomness.createSecure());
                } catch (NoSuchAlgorithmException | KeyManagementException | NoSuchProviderException e) {
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
                        return List.of();
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
