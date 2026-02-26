/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.OSFlightClient;
import org.apache.arrow.flight.OSFlightServer;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.lang.reflect.Field;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.grpc.ManagedChannel;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link DefaultSslContextProvider} picks up a new certificate after the underlying
 * {@link SSLContext} is re-initialized with new key material, without restarting the Flight
 * server or client.
 * <p>
 * {@code SSLContext.init()} replaces the key/trust material on the same live object. Since
 * {@link io.netty.handler.ssl.JdkSslContext} holds a reference to that object and calls
 * {@code createSSLEngine()} on it per connection, the next handshake after re-init uses the
 * new certificate automatically.
 * <p>
 * {@code ManagedChannel.enterIdle()} drops the current TCP connection so the next RPC call
 * triggers a fresh TLS handshake on the same client instance.
 */
public class DefaultSslContextProviderFlightIT extends OpenSearchTestCase {

    public void testHotReloadWithoutRestart() throws Exception {
        X509Bundle cert1 = new CertificateBuilder().subject("CN=cert1.example.com").setIsCertificateAuthority(true).buildSelfSigned();
        X509Bundle cert2 = new CertificateBuilder().subject("CN=cert2.example.com").setIsCertificateAuthority(true).buildSelfSigned();

        // Single SSLContext instance — re-initialized in-place to simulate reloadcerts
        SSLContext liveSslContext = SSLContext.getInstance("TLS");
        liveSslContext.init(cert1.toKeyManagerFactory().getKeyManagers(), null, null);

        X509Certificate[] capturedChain = { null };
        TrustManager capturingTm = capturingTrustManager(capturedChain);

        SecureTransportSettingsProvider.SecureTransportParameters params = mock(
            SecureTransportSettingsProvider.SecureTransportParameters.class
        );
        when(params.clientAuth()).thenReturn(Optional.of("NONE"));
        when(params.cipherSuites()).thenReturn(List.of());

        SecureTransportSettingsProvider serverSettingsProvider = mock(SecureTransportSettingsProvider.class);
        when(serverSettingsProvider.buildSecureTransportContext(any())).thenReturn(Optional.of(liveSslContext));
        when(serverSettingsProvider.parameters(any())).thenReturn(Optional.of(params));

        SSLContext clientSslContext = SSLContext.getInstance("TLS");
        clientSslContext.init(null, new TrustManager[] { capturingTm }, null);

        SecureTransportSettingsProvider clientSettingsProvider = mock(SecureTransportSettingsProvider.class);
        when(clientSettingsProvider.buildSecureTransportContext(any())).thenReturn(Optional.of(clientSslContext));
        when(clientSettingsProvider.parameters(any())).thenReturn(Optional.of(params));

        Settings settings = Settings.builder().put("transport.ssl.enforce_hostname_verification", false).build();

        // Both SslContext objects built once — JdkSslContext holds reference to liveSslContext
        DefaultSslContextProvider serverProvider = new DefaultSslContextProvider(serverSettingsProvider, settings);
        DefaultSslContextProvider clientProvider = new DefaultSslContextProvider(clientSettingsProvider, settings);

        int port = getBasePort(9600) + randomIntBetween(0, 99);
        Location location = Location.forGrpcTls("localhost", port);
        ExecutorService exec = Executors.newSingleThreadExecutor();

        try (RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            FlightServer server = OSFlightServer.builder()
                .allocator(allocator.newChildAllocator("server", 0, Long.MAX_VALUE))
                .location(location)
                .producer(new NoOpFlightProducer())
                .sslContext(serverProvider.getServerSslContext())
                .executor(exec)
                .build();
            server.start();

            try (
                FlightClient client = OSFlightClient.builder()
                    .allocator(allocator.newChildAllocator("client", 0, Long.MAX_VALUE))
                    .location(location)
                    .sslContext(clientProvider.getClientSslContext())
                    .build()
            ) {
                try {
                    triggerHandshake(client);
                    assertEquals("cert1.example.com", getCN(capturedChain));

                    // Simulate reloadcerts: re-initialize the same SSLContext object with new key material
                    liveSslContext.init(cert2.toKeyManagerFactory().getKeyManagers(), null, null);

                    getChannel(client).enterIdle();

                    triggerHandshake(client);
                    assertEquals("cert2.example.com", getCN(capturedChain));
                } finally {
                    server.shutdown();
                    server.awaitTermination();
                    server.close();
                    exec.shutdownNow();
                }
            }
        }
    }

    // ---- helpers ----

    private static void triggerHandshake(FlightClient client) {
        try {
            client.listFlights(Criteria.ALL).forEach(f -> {});
        } catch (Exception ignored) {} // NoOpFlightProducer throws UNIMPLEMENTED; handshake already done
    }

    @SuppressForbidden(reason = "need access to FlightClient's private channel field for test verification")
    private static ManagedChannel getChannel(FlightClient client) throws Exception {
        Field f = FlightClient.class.getDeclaredField("channel");
        f.setAccessible(true);
        return (ManagedChannel) f.get(client);
    }

    private static String getCN(X509Certificate[] chain) {
        assertNotNull("TrustManager was never called — handshake did not occur", chain[0]);
        return chain[0].getSubjectX500Principal().getName().replaceFirst(".*CN=([^,]+).*", "$1");
    }

    private static TrustManager capturingTrustManager(X509Certificate[] capturedChain) {
        return new X509ExtendedTrustManager() {
            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
                capturedChain[0] = chain[0];
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
                capturedChain[0] = chain[0];
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                capturedChain[0] = chain[0];
            }

            @Override
            public void checkClientTrusted(X509Certificate[] c, String a, SSLEngine e) {}

            @Override
            public void checkClientTrusted(X509Certificate[] c, String a, Socket s) {}

            @Override
            public void checkClientTrusted(X509Certificate[] c, String a) throws CertificateException {}

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
    }
}
