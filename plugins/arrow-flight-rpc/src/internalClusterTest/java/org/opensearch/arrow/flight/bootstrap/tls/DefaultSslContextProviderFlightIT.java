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
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import io.grpc.ManagedChannel;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link DefaultSslContextProvider} picks up a new certificate after cert reload,
 * without restarting the Flight server or client.
 * <p>
 * Two reload scenarios are tested:
 * <ol>
 *   <li><b>In-place mutation</b>: {@code SSLContext.init()} replaces key material on the same instance.</li>
 *   <li><b>New instance</b>: {@link SecureTransportSettingsProvider#buildSecureTransportContext}
 *       returns a brand-new {@link SSLContext} on each call. This requires
 *       {@link DefaultSslContextProvider} to invoke {@code buildSecureTransportContext} on every
 *       {@code newEngine()} call rather than caching the result from startup.</li>
 * </ol>
 * {@code ManagedChannel.enterIdle()} drops the current TCP connection so the next RPC call
 * triggers a fresh TLS handshake on the same client instance.
 */
public class DefaultSslContextProviderFlightIT extends OpenSearchTestCase {

    private Locale savedLocale;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // CertificateBuilder uses BouncyCastle's SimpleDateFormat for UTCTime, which is
        // locale-sensitive. Non-Latin locales produce non-ASCII date strings that BouncyCastle
        // cannot parse back, causing test failures. Pin to Locale.ROOT for the duration.
        savedLocale = Locale.getDefault();
        Locale.setDefault(Locale.ROOT);
    }

    @Override
    public void tearDown() throws Exception {
        Locale.setDefault(savedLocale);
        super.tearDown();
    }

    /** Cert reload via in-place {@code SSLContext.init()} on the same instance. */
    public void testHotReloadInPlace() throws Exception {
        X509Bundle cert1 = selfSigned("cert1.example.com");
        X509Bundle cert2 = selfSigned("cert2.example.com");

        SSLContext serverCtx = SSLContext.getInstance("TLS");
        serverCtx.init(cert1.toKeyManagerFactory().getKeyManagers(), null, null);

        runReloadTest(9600, () -> serverCtx, () -> {
            try {
                serverCtx.init(cert2.toKeyManagerFactory().getKeyManagers(), null, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, "cert1.example.com", "cert2.example.com");
    }

    /** Cert reload where the provider returns a new {@link SSLContext} instance on each call. */
    public void testHotReloadWithNewInstance() throws Exception {
        X509Bundle cert1 = selfSigned("cert1.example.com");
        X509Bundle cert2 = selfSigned("cert2.example.com");

        SSLContext ctx1 = SSLContext.getInstance("TLS");
        ctx1.init(cert1.toKeyManagerFactory().getKeyManagers(), null, null);
        SSLContext ctx2 = SSLContext.getInstance("TLS");
        ctx2.init(cert2.toKeyManagerFactory().getKeyManagers(), null, null);

        SSLContext[] current = { ctx1 };
        runReloadTest(9700, () -> current[0], () -> current[0] = ctx2, "cert1.example.com", "cert2.example.com");
    }

    // ---- core test logic ----

    private void runReloadTest(int basePort, Supplier<SSLContext> serverCtxSupplier, Runnable reload, String cnBefore, String cnAfter)
        throws Exception {
        X509Certificate[] capturedChain = { null };
        TrustManager capturingTm = capturingTrustManager(capturedChain);

        SecureTransportSettingsProvider.SecureTransportParameters params = mock(
            SecureTransportSettingsProvider.SecureTransportParameters.class
        );
        when(params.clientAuth()).thenReturn(Optional.of("NONE"));
        when(params.cipherSuites()).thenReturn(List.of());

        SecureTransportSettingsProvider serverProvider = mock(SecureTransportSettingsProvider.class);
        when(serverProvider.buildSecureTransportContext(any())).thenAnswer(inv -> Optional.of(serverCtxSupplier.get()));
        when(serverProvider.parameters(any())).thenReturn(Optional.of(params));

        SSLContext clientCtx = SSLContext.getInstance("TLS");
        clientCtx.init(null, new TrustManager[] { capturingTm }, null);

        SecureTransportSettingsProvider clientProvider = mock(SecureTransportSettingsProvider.class);
        when(clientProvider.buildSecureTransportContext(any())).thenReturn(Optional.of(clientCtx));
        when(clientProvider.parameters(any())).thenReturn(Optional.of(params));

        Settings settings = Settings.builder().put("transport.ssl.enforce_hostname_verification", false).build();

        DefaultSslContextProvider sslServer = new DefaultSslContextProvider(serverProvider, settings);
        DefaultSslContextProvider sslClient = new DefaultSslContextProvider(clientProvider, settings);

        int port = getBasePort(basePort) + randomIntBetween(0, 99);
        Location location = Location.forGrpcTls("localhost", port);
        ExecutorService exec = Executors.newSingleThreadExecutor();

        try (RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            FlightServer server = OSFlightServer.builder()
                .allocator(allocator.newChildAllocator("server", 0, Long.MAX_VALUE))
                .location(location)
                .producer(new NoOpFlightProducer())
                .sslContext(sslServer.getServerSslContext())
                .executor(exec)
                .build();
            server.start();

            try (
                FlightClient client = OSFlightClient.builder()
                    .allocator(allocator.newChildAllocator("client", 0, Long.MAX_VALUE))
                    .location(location)
                    .sslContext(sslClient.getClientSslContext())
                    .build()
            ) {
                try {
                    triggerHandshake(client);
                    assertEquals(cnBefore, getCN(capturedChain));

                    reload.run();
                    getChannel(client).enterIdle();

                    triggerHandshake(client);
                    assertEquals(cnAfter, getCN(capturedChain));
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

    private static X509Bundle selfSigned(String cn) throws Exception {
        return new CertificateBuilder().subject("CN=" + cn).setIsCertificateAuthority(true).buildSelfSigned();
    }

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
