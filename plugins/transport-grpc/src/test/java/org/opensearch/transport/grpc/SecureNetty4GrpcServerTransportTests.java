/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import io.grpc.BindableService;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

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

import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;

public class SecureNetty4GrpcServerTransportTests extends OpenSearchTestCase {
    private TestThreadPool threadPool;
    private NetworkService networkService;
    private final List<BindableService> services = new ArrayList<>();
    private SecureAuxTransportSettingsProvider settingsProvider;

    static class TestSecureAuxTransportSettingsProvider implements SecureAuxTransportSettingsProvider {
        @Override
        public Optional<SSLContext> buildSecureAuxServerSSLContext(Settings settings, NetworkPlugin.AuxTransport transport) throws SSLException {
            try {
                final KeyStore keyStore = KeyStore.getInstance("PKCS12");
                keyStore.load(
                    SecureNetty4GrpcServerTransport.class.getResourceAsStream("/netty4-secure.jks"),
                    "password".toCharArray()
                );

                final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                keyManagerFactory.init(keyStore, "password".toCharArray());

                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
                return Optional.of(sslContext);
            } catch (final IOException |
                           NoSuchAlgorithmException |
                           UnrecoverableKeyException |
                           KeyStoreException |
                           CertificateException |
                           KeyManagementException ex) {
                throw new SSLException(ex);
            }
        }
    }

    @Before
    public void setup() {
        threadPool = new TestThreadPool("test");
        networkService = new NetworkService(Collections.emptyList());
        settingsProvider = new TestSecureAuxTransportSettingsProvider();
    }

    @After
    public void shutdown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
    }

    private static Settings createSettings() {
        return Settings.builder().put(
            SecureNetty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(),
            getPortRange())
            .build();
    }

    public void testGrpcSecureTransportStartStop() {
        try (SecureNetty4GrpcServerTransport serverTransport = new SecureNetty4GrpcServerTransport(
            createSettings(),
            services,
            networkService,
            settingsProvider
        )) {
            serverTransport.start();
            MatcherAssert.assertThat(serverTransport.boundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(serverTransport.boundAddress().publishAddress().address());
            serverTransport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testGrpcSecureTransportHealthcheck() {
        try (SecureNetty4GrpcServerTransport serverTransport = new SecureNetty4GrpcServerTransport(
                createSettings(),
                services,
                networkService,
                settingsProvider
            )) {
                serverTransport.start();
                final TransportAddress remoteAddress = randomFrom(serverTransport.boundAddress().boundAddresses());

                NettyGrpcClient client = new NettyGrpcClient.Builder()
                    .setAddress(remoteAddress)
                    .setTls(false)
                    .build();

                client.checkHealth();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
