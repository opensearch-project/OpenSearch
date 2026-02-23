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
import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.lang.reflect.Field;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.ManagedChannel;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

/**
 * Verifies that a single {@link FlightClient} and {@link FlightServer}, both built once with
 * {@link ReloadableSslContext}, pick up a new certificate after the supplier is swapped —
 * without restarting either side.
 * <p>
 * {@code ManagedChannel.enterIdle()} drops the current TCP connection. The next call on the
 * same client triggers a reconnect, causing gRPC's {@code ClientTlsHandler} to call
 * {@code newEngine()} on the same {@link ReloadableSslContext} instance, which now reads the
 * new cert from the supplier. The server side works identically via {@code ServerTlsHandler}.
 */
public class ReloadableSslContextFlightIT extends OpenSearchTestCase {

    public void testHotReloadWithoutRestart() throws Exception {
        X509Bundle cert1 = new CertificateBuilder().subject("CN=cert1.example.com").setIsCertificateAuthority(true).buildSelfSigned();
        X509Bundle cert2 = new CertificateBuilder().subject("CN=cert2.example.com").setIsCertificateAuthority(true).buildSelfSigned();

        AtomicReference<SslContext> serverSupplier = new AtomicReference<>(buildServerContext(cert1));
        ReloadableSslContext serverReloadable = new ReloadableSslContext(serverSupplier.get(), serverSupplier::get);

        AtomicReference<X509Certificate[]> capturedChain = new AtomicReference<>();
        AtomicReference<SslContext> clientSupplier = new AtomicReference<>(buildCapturingClientContext(capturedChain));
        ReloadableSslContext clientReloadable = new ReloadableSslContext(clientSupplier.get(), clientSupplier::get);

        int port = getBasePort(9600) + randomIntBetween(0, 99);
        Location location = Location.forGrpcTls("localhost", port);
        ExecutorService exec = Executors.newSingleThreadExecutor();

        try (RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            // Server built and started ONCE
            FlightServer server = OSFlightServer.builder()
                .allocator(allocator.newChildAllocator("server", 0, Long.MAX_VALUE))
                .location(location)
                .producer(new NoOpFlightProducer())
                .sslContext(serverReloadable)
                .executor(exec)
                .build();
            server.start();

            // Client built ONCE
            try (
                FlightClient client = OSFlightClient.builder()
                    .allocator(allocator.newChildAllocator("client", 0, Long.MAX_VALUE))
                    .location(location)
                    .sslContext(clientReloadable)
                    .build()
            ) {

                try {
                    // Handshake 1: both sides call newEngine() on their ReloadableSslContext → cert1
                    triggerHandshake(client);
                    assertEquals("cert1.example.com", getCN(capturedChain));

                    // Simulate reloadcerts: swap both suppliers (no restart of server or client)
                    serverSupplier.set(buildServerContext(cert2));
                    clientSupplier.set(buildCapturingClientContext(capturedChain));

                    // Force the existing gRPC channel to drop its TCP connection and reconnect
                    getChannel(client).enterIdle();

                    // Handshake 2: same client, same server, same ReloadableSslContext instances
                    // → newEngine() called again → cert2
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

    /** Extracts the underlying {@link ManagedChannel} from a {@link FlightClient} via reflection. */
    @SuppressForbidden(reason = "need access to FlightClient's private channel field for test verification")
    private static ManagedChannel getChannel(FlightClient client) throws Exception {
        Field f = FlightClient.class.getDeclaredField("channel");
        f.setAccessible(true);
        return (ManagedChannel) f.get(client);
    }

    private static String getCN(AtomicReference<X509Certificate[]> capturedChain) {
        X509Certificate[] chain = capturedChain.get();
        assertNotNull("TrustManager was never called — handshake did not occur", chain);
        return chain[0].getSubjectX500Principal().getName().replaceFirst(".*CN=([^,]+).*", "$1");
    }

    private static SslContext buildServerContext(X509Bundle cert) throws Exception {
        return SslContextBuilder.forServer(cert.toKeyManagerFactory())
            .clientAuth(ClientAuth.NONE)
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocolConfig(alpn())
            .build();
    }

    private static SslContext buildCapturingClientContext(AtomicReference<X509Certificate[]> capturedChain) throws Exception {
        TrustManager capturingTm = new X509ExtendedTrustManager() {
            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
                capturedChain.set(chain);
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
                capturedChain.set(chain);
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                capturedChain.set(chain);
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

        return SslContextBuilder.forClient().trustManager(capturingTm).applicationProtocolConfig(alpn()).build();
    }

    private static ApplicationProtocolConfig alpn() {
        return new ApplicationProtocolConfig(
            ApplicationProtocolConfig.Protocol.ALPN,
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
            ApplicationProtocolNames.HTTP_2
        );
    }
}
