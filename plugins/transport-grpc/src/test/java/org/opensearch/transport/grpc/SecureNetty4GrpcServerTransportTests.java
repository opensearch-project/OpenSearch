/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import io.grpc.BindableService;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class SecureNetty4GrpcServerTransportTests extends OpenSearchTestCase {
    private static final String PROVIDER = "JDK"; // only guaranteed provider
    private static final String[] DEFAULT_SSL_PROTOCOLS = { "TLSv1.3", "TLSv1.2", "TLSv1.1" };
    private static final String[] DEFAULT_CIPHERS = {
        "TLS_AES_128_GCM_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" };

    private NetworkService networkService;
    private final List<BindableService> services = new ArrayList<>();

    private static KeyManagerFactory getTestKeyManagerFactory() {
        KeyManagerFactory keyManagerFactory;
        try {
            final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(SecureNetty4GrpcServerTransport.class.getResourceAsStream("/netty4-secure.jks"), "password".toCharArray());
            keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, "password".toCharArray());
        } catch (UnrecoverableKeyException | CertificateException | KeyStoreException | IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return keyManagerFactory;
    }

    static SecureAuxTransportSettingsProvider getSecureSettingsProviderServer(
        String provider,
        String clientAuth,
        Collection<String> protocols,
        Collection<String> cipherSuites,
        KeyManagerFactory keyMngerFactory,
        TrustManagerFactory trustMngerFactory
    ) {
        return new SecureAuxTransportSettingsProvider() {
            @Override
            public Optional<SecureAuxTransportSettingsProvider.SecureAuxTransportParameters> parameters() {
                return Optional.of(new SecureAuxTransportSettingsProvider.SecureAuxTransportParameters() {
                    @Override
                    public Optional<String> sslProvider() {
                        return Optional.of(provider);
                    }

                    @Override
                    public Optional<String> clientAuth() {
                        return Optional.of(clientAuth);
                    }

                    @Override
                    public Collection<String> protocols() {
                        return protocols;
                    }

                    @Override
                    public Collection<String> cipherSuites() {
                        return cipherSuites;
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

    static SecureAuxTransportSettingsProvider getNoTrustClientAuthNoneTLSSettingsProvider() {
        return getSecureSettingsProviderServer(
            PROVIDER,
            ClientAuth.NONE.name().toUpperCase(Locale.ROOT),
            List.of(DEFAULT_SSL_PROTOCOLS),
            List.of(DEFAULT_CIPHERS),
            getTestKeyManagerFactory(),
            InsecureTrustManagerFactory.INSTANCE
        );
    }

    static Settings createSettings() {
        return Settings.builder().put(SecureNetty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), getPortRange()).build();
    }

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
    }

    @After
    public void shutdown() {
        networkService = null;
    }

    public void testGrpcSecureTransportStartStop() {
        try (
            SecureNetty4GrpcServerTransport transport = new SecureNetty4GrpcServerTransport(
                createSettings(),
                services,
                networkService,
                getNoTrustClientAuthNoneTLSSettingsProvider()
            )
        ) {
            transport.start();
            assertTrue(transport.getBoundAddress().boundAddresses().length > 0);
            assertNotNull(transport.getBoundAddress().publishAddress().address());
            transport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testGrpcSecureTransportHealthcheck() {
        try (
            SecureNetty4GrpcServerTransport transport = new SecureNetty4GrpcServerTransport(
                createSettings(),
                services,
                networkService,
                getNoTrustClientAuthNoneTLSSettingsProvider()
            )
        ) {
            transport.start();
            assertTrue(transport.getBoundAddress().boundAddresses().length > 0);
            assertNotNull(transport.getBoundAddress().publishAddress().address());
            final TransportAddress remoteAddress = randomFrom(transport.getBoundAddress().boundAddresses());
            try (
                NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(remoteAddress)
                    .setSecureSettingsProvider(getNoTrustClientAuthNoneTLSSettingsProvider())
                    .build()
            ) {
                assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
            }
            transport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
