/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;

import java.util.List;
import java.util.function.Supplier;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.netty.handler.ssl.SslContext;

/**
 * A {@link SslContext} that reads a fresh delegate on every {@code newEngine()} call.
 * <p>
 * This mirrors exactly how {@code SecureNetty4Transport} works: rather than caching a {@link SslContext}
 * at startup, it calls the supplier each time a new TLS engine is needed. The supplier reads from
 * {@code SslContextHandler.sslContext} — the live field that the security plugin's
 * {@code reloadcerts} API replaces — so any new connection after a cert reload automatically uses
 * the updated certificate material with no restart and no explicit reload hook.
 * <p>
 * The initial context is used for stable metadata methods (cipher suites, ALPN config, etc.).
 * These are read once at startup and do not change across reloads.
 */
final class ReloadableSslContext extends SslContext {

    private final SslContext initial;
    private final Supplier<SslContext> contextSupplier;

    ReloadableSslContext(SslContext initial, Supplier<SslContext> contextSupplier) {
        this.initial = initial;
        this.contextSupplier = contextSupplier;
    }

    /**
     * Called by gRPC's {@code ServerTlsHandler.handlerAdded()} and {@code ClientTlsHandler.handlerAdded0()}
     * on every new inbound/outbound TCP connection. Reads a fresh {@link SslContext} from the supplier
     * each time, so cert reloads take effect immediately on the next connection.
     */
    @Override
    public SSLEngine newEngine(ByteBufAllocator alloc) {
        return contextSupplier.get().newEngine(alloc);
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator alloc, String peerHost, int peerPort) {
        return contextSupplier.get().newEngine(alloc, peerHost, peerPort);
    }

    @Override
    public boolean isClient() {
        return initial.isClient();
    }

    @Override
    public List<String> cipherSuites() {
        return initial.cipherSuites();
    }

    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return initial.applicationProtocolNegotiator();
    }

    @Override
    public SSLSessionContext sessionContext() {
        return initial.sessionContext();
    }
}
