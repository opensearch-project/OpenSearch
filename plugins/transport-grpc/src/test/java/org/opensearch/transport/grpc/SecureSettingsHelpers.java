/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import static org.opensearch.transport.grpc.SecureSettingsHelpers.ConnectExceptions.BAD_CERT;
import static org.opensearch.transport.grpc.SecureSettingsHelpers.ConnectExceptions.CERT_REQUIRED;
import static org.opensearch.transport.grpc.SecureSettingsHelpers.ConnectExceptions.UNAVAILABLE;

public class SecureSettingsHelpers {
    private static final String PROVIDER = "JDK"; // only guaranteed provider
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
     */
    protected enum ConnectExceptions {
        NONE("Connection succeeded"),
        UNAVAILABLE("Network closed for unknown reason"),
        BAD_CERT("bad_certificate"),
        CERT_REQUIRED("certificate_required");

        String exceptionMsg = null;

        ConnectExceptions(String exceptionMsg) {
            this.exceptionMsg = exceptionMsg;
        }
    }

    static ConnectExceptions FailurefromException(Exception e) throws Exception {
        if (e instanceof SSLException || e instanceof StatusRuntimeException) {
            if (e.getMessage() != null && e.getMessage().contains(BAD_CERT.exceptionMsg)) {
                return BAD_CERT;
            }
            if (e.getCause() != null && e.getCause().getMessage().contains(BAD_CERT.exceptionMsg)) {
                return BAD_CERT;
            }
            if (e.getCause() != null && e.getCause().getMessage().contains(CERT_REQUIRED.exceptionMsg)) {
                return CERT_REQUIRED;
            }
        }
        if (e.getMessage() != null && e.getMessage().contains(UNAVAILABLE.exceptionMsg)) {
            return UNAVAILABLE;
        }
        throw e;
    }

    static KeyManagerFactory getTestKeyManagerFactory(String keystorePath) {
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
            public Optional<SecureAuxTransportParameters> parameters() {
                return Optional.of(new SecureAuxTransportSettingsProvider.SecureAuxTransportParameters() {
                    @Override
                    public Optional<String> sslProvider() {
                        return Optional.of(PROVIDER);
                    }

                    @Override
                    public Optional<String> clientAuth() {
                        return Optional.of(clientAuth);
                    }

                    @Override
                    public Collection<String> protocols() {
                        return List.of(DEFAULT_SSL_PROTOCOLS);
                    }

                    @Override
                    public Collection<String> cipherSuites() {
                        return List.of(DEFAULT_CIPHERS);
                    }

                    @Override
                    public Optional<KeyManagerFactory> keyManagerFactory() {
                        return Optional.of(keyMngerFactory);
                    }

                    @Override
                    public Optional<TrustManagerFactory> trustManagerFactory() {
                        return Optional.of(trustMngerFactory);
                    }
                });
            }
        };
    }

    static SecureAuxTransportSettingsProvider getServerClientAuthRequired() {
        return getSecureSettingsProvider(
            ClientAuth.REQUIRE.name().toUpperCase(Locale.ROOT),
            getTestKeyManagerFactory(SERVER_KEYSTORE),
            getTestTrustManagerFactory(CLIENT_KEYSTORE)
        );
    }

    static SecureAuxTransportSettingsProvider getServerClientAuthOptional() {
        return getSecureSettingsProvider(
            ClientAuth.OPTIONAL.name().toUpperCase(Locale.ROOT),
            getTestKeyManagerFactory(SERVER_KEYSTORE),
            getTestTrustManagerFactory(CLIENT_KEYSTORE)
        );
    }

    static SecureAuxTransportSettingsProvider getServerClientAuthNone() {
        return getSecureSettingsProvider(
            ClientAuth.NONE.name().toUpperCase(Locale.ROOT),
            getTestKeyManagerFactory(SERVER_KEYSTORE),
            InsecureTrustManagerFactory.INSTANCE
        );
    }
}
