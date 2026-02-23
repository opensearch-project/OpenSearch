/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source source.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.netty.handler.ssl.SslContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link ReloadableSslContext} delegates {@code newEngine()} to the current
 * supplier value, so cert reloads take effect on the next connection.
 */
public class ReloadableSslContextTests extends OpenSearchTestCase {

    public void testNewEnginePicksUpReloadedCert() {
        SSLEngine engine1 = mock(SSLEngine.class);
        SSLEngine engine2 = mock(SSLEngine.class);

        SslContext ctx1 = stubContext(engine1);
        SslContext ctx2 = stubContext(engine2);

        AtomicReference<SslContext> current = new AtomicReference<>(ctx1);
        ReloadableSslContext reloadable = new ReloadableSslContext(ctx1, current::get);

        assertSame(engine1, reloadable.newEngine(ByteBufAllocator.DEFAULT));

        current.set(ctx2);

        assertSame(engine2, reloadable.newEngine(ByteBufAllocator.DEFAULT));
    }

    public void testNewEngineWithPeerPicksUpReloadedContext() {
        SSLEngine engine1 = mock(SSLEngine.class);
        SSLEngine engine2 = mock(SSLEngine.class);

        SslContext ctx1 = stubContextWithPeer(engine1);
        SslContext ctx2 = stubContextWithPeer(engine2);

        AtomicReference<SslContext> current = new AtomicReference<>(ctx1);
        ReloadableSslContext reloadable = new ReloadableSslContext(ctx1, current::get);

        assertSame(engine1, reloadable.newEngine(ByteBufAllocator.DEFAULT, "host", 443));

        current.set(ctx2);

        assertSame(engine2, reloadable.newEngine(ByteBufAllocator.DEFAULT, "host", 443));
    }

    public void testMetadataDelegatestoInitial() {
        SslContext initial = stubContext(mock(SSLEngine.class));
        ReloadableSslContext reloadable = new ReloadableSslContext(initial, () -> initial);

        assertSame(initial.isClient(), reloadable.isClient());
        assertSame(initial.cipherSuites(), reloadable.cipherSuites());
        assertSame(initial.sessionContext(), reloadable.sessionContext());
        assertSame(initial.applicationProtocolNegotiator(), reloadable.applicationProtocolNegotiator());
    }

    // ---- helpers ----

    private static SslContext stubContext(SSLEngine engine) {
        SslContext ctx = mock(SslContext.class);
        when(ctx.newEngine(ByteBufAllocator.DEFAULT)).thenReturn(engine);
        when(ctx.isClient()).thenReturn(false);
        when(ctx.cipherSuites()).thenReturn(List.of());
        when(ctx.sessionContext()).thenReturn(mock(SSLSessionContext.class));
        when(ctx.applicationProtocolNegotiator()).thenReturn(mock(ApplicationProtocolNegotiator.class));
        return ctx;
    }

    private static SslContext stubContextWithPeer(SSLEngine engine) {
        SslContext ctx = mock(SslContext.class);
        when(ctx.newEngine(ByteBufAllocator.DEFAULT, "host", 443)).thenReturn(engine);
        return ctx;
    }
}
