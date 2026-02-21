/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLSession;

import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

/**
 * Verifies that {@link ReloadableSslContext} picks up a new {@link SslContext} on each
 * {@code newEngine()} call, simulating what happens when {@code reloadcerts} replaces the
 * live {@code SslContextHandler.sslContext} field.
 * <p>
 * The test performs two real in-JVM TLS handshakes — one before and one after swapping the
 * supplier — and checks that the server certificate presented to the client changes.
 */
public class ReloadableSslContextTests extends OpenSearchTestCase {

    public void testNewEnginePicksUpReloadedCert() throws Exception {
        // Two self-signed certs with distinct CNs to tell them apart after handshake
        X509Bundle cert1 = new CertificateBuilder().subject("CN=cert1.example.com").setIsCertificateAuthority(true).buildSelfSigned();
        X509Bundle cert2 = new CertificateBuilder().subject("CN=cert2.example.com").setIsCertificateAuthority(true).buildSelfSigned();

        SslContext ctx1 = buildServerContext(cert1);
        SslContext ctx2 = buildServerContext(cert2);

        AtomicReference<SslContext> current = new AtomicReference<>(ctx1);
        ReloadableSslContext reloadable = new ReloadableSslContext(ctx1, current::get);

        // Before reload: server presents cert1
        String cn1 = handshakeAndGetServerCN(reloadable);
        assertEquals("cert1.example.com", cn1);

        // Simulate cert reload — swap the supplier's backing context
        current.set(ctx2);

        // After reload: server presents cert2 on the next (new) connection
        String cn2 = handshakeAndGetServerCN(reloadable);
        assertEquals("cert2.example.com", cn2);
    }

    // ---- helpers ----

    private static SslContext buildServerContext(X509Bundle cert) throws Exception {
        return SslContextBuilder.forServer(cert.toKeyManagerFactory())
            .clientAuth(ClientAuth.NONE)
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocolConfig(
                new ApplicationProtocolConfig(
                    ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    ApplicationProtocolNames.HTTP_2
                )
            )
            .build();
    }

    /**
     * Performs a minimal in-JVM TLS handshake between a server engine from {@code serverCtx}
     * and a client engine that trusts everything, then returns the CN of the server certificate
     * presented to the client.
     */
    private static String handshakeAndGetServerCN(SslContext serverCtx) throws Exception {
        SSLEngine server = serverCtx.newEngine(ByteBufAllocator.DEFAULT);
        server.setUseClientMode(false);

        SslContext clientCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        SSLEngine client = clientCtx.newEngine(ByteBufAllocator.DEFAULT, "localhost", 0);
        client.setUseClientMode(true);

        runHandshake(client, server);

        SSLSession session = client.getSession();
        X509Certificate serverCert = (X509Certificate) session.getPeerCertificates()[0];
        String dn = serverCert.getSubjectX500Principal().getName();
        return dn.replaceFirst(".*CN=([^,]+).*", "$1");
    }

    /**
     * Drives the TLS handshake between two {@link SSLEngine}s entirely in memory.
     */
    private static void runHandshake(SSLEngine client, SSLEngine server) throws Exception {
        int bufSize = client.getSession().getPacketBufferSize() * 2;
        ByteBuffer clientOut = ByteBuffer.allocate(bufSize);
        ByteBuffer serverOut = ByteBuffer.allocate(bufSize);
        ByteBuffer empty = ByteBuffer.allocate(0);
        ByteBuffer appBuf = ByteBuffer.allocate(bufSize);

        client.beginHandshake();
        server.beginHandshake();

        for (int i = 0; i < 20; i++) {
            // client → server
            clientOut.clear();
            client.wrap(empty, clientOut);
            clientOut.flip();
            if (clientOut.hasRemaining()) {
                appBuf.clear();
                server.unwrap(clientOut, appBuf);
            }

            // server → client
            serverOut.clear();
            server.wrap(empty, serverOut);
            serverOut.flip();
            if (serverOut.hasRemaining()) {
                appBuf.clear();
                client.unwrap(serverOut, appBuf);
            }

            if (client.getHandshakeStatus() == HandshakeStatus.FINISHED || client.getHandshakeStatus() == HandshakeStatus.NOT_HANDSHAKING) {
                break;
            }

            runDelegatedTasks(client);
            runDelegatedTasks(server);
        }
    }

    private static void runDelegatedTasks(SSLEngine engine) {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
    }
}
