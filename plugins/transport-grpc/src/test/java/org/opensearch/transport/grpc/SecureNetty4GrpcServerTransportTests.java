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
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.grpc.BindableService;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class SecureNetty4GrpcServerTransportTests extends OpenSearchTestCase {
    private NetworkService networkService;
    private final List<BindableService> services = new ArrayList<>();
    private SecureAuxTransportSettingsProvider settingsProvider;

    private static SecureAuxTransportSettingsProvider getSecureSettingsProvider() {
        return () -> {
            /**
             * Attempt to fetch some supported cipher suite from default provider.
             * Else fall back to some common hard coded defaults.
             */
            List<String> cipherSuites;
            try {
                SSLContext defaultContext = SSLContext.getDefault();
                SSLEngine tempEngine = defaultContext.createSSLEngine();
                cipherSuites = List.of(tempEngine.getSupportedCipherSuites());
            } catch (NoSuchAlgorithmException e) {
                cipherSuites = List.of(
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",  // TLSv1.2
                    "TLS_AES_128_GCM_SHA256"                  // TLSv1.3
                );
            }

            /**
             * Init keystore from test resources.
             */
            KeyManagerFactory keyManagerFactory;
            try {
                final KeyStore keyStore = KeyStore.getInstance("PKCS12");
                keyStore.load(SecureNetty4GrpcServerTransport.class.getResourceAsStream("/netty4-secure.jks"), "password".toCharArray());
                keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                keyManagerFactory.init(keyStore, "password".toCharArray());
            } catch (UnrecoverableKeyException | CertificateException | KeyStoreException | IOException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            SSLContext sslContext;
            try {
                sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagerFactory.getKeyManagers(), InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null); // no random for tests
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                throw new RuntimeException("Failed to initialize SSL context", e);
            }

            SSLEngine engine = sslContext.createSSLEngine();
            engine.setEnabledProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
            engine.setEnabledCipherSuites(cipherSuites.toArray(new String[0]));
            engine.setNeedClientAuth(false);
            engine.setWantClientAuth(false);

            // Set ALPN (for HTTP/2 support)
            SSLParameters params = engine.getSSLParameters();
            params.setApplicationProtocols(new String[]{"h2"});
            engine.setSSLParameters(params);

            return Optional.of(engine);
        };
    }

    static Settings createSettings() {
        return Settings.builder().put(SecureNetty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), getPortRange()).build();
    }

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
        settingsProvider = getSecureSettingsProvider();
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
                settingsProvider
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
                settingsProvider
            )
        ) {
            transport.start();
            assertTrue(transport.getBoundAddress().boundAddresses().length > 0);
            assertNotNull(transport.getBoundAddress().publishAddress().address());
            final TransportAddress remoteAddress = randomFrom(transport.getBoundAddress().boundAddresses());
            try (
                NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(remoteAddress)
                    .setSecureSettingsProvider(settingsProvider)
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
