/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.util.List;
import java.util.Optional;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.AlpnAwareSSLEngineWrapper;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DefaultSslContextProvider} and {@link AlpnAwareSSLEngineWrapper}.
 */
public class DefaultSslContextProviderTests extends OpenSearchTestCase {

    // ---- AlpnAwareSSLEngineWrapper ----

    public void testWrapperStripsEndpointIdentificationAlgorithm() {
        SSLEngine inner = mock(SSLEngine.class);
        SSLParameters[] captured = { null };
        doAnswer(inv -> {
            captured[0] = inv.getArgument(0);
            return null;
        }).when(inner).setSSLParameters(any());

        AlpnAwareSSLEngineWrapper wrapper = new AlpnAwareSSLEngineWrapper(inner);
        SSLParameters p = new SSLParameters();
        p.setEndpointIdentificationAlgorithm("HTTPS");
        wrapper.setSSLParameters(p);

        assertNotNull(captured[0]);
        assertEquals("endpointIdentificationAlgorithm must be stripped", "", captured[0].getEndpointIdentificationAlgorithm());
    }

    public void testWrapperDelegatesGetNegotiatedApplicationProtocol() {
        SSLEngine inner = mock(SSLEngine.class);
        when(inner.getApplicationProtocol()).thenReturn(ApplicationProtocolNames.HTTP_2);

        AlpnAwareSSLEngineWrapper wrapper = new AlpnAwareSSLEngineWrapper(inner);
        assertEquals(ApplicationProtocolNames.HTTP_2, wrapper.getNegotiatedApplicationProtocol());
    }

    // ---- DefaultSslContextProvider ----

    public void testGetServerSslContextIsNotClientMode() throws Exception {
        SslContext ctx = providerWithRealSslContext(false).getServerSslContext();
        assertNotNull(ctx);
        assertFalse(ctx.isClient());
    }

    public void testGetClientSslContextIsClientMode() throws Exception {
        SslContext ctx = providerWithRealSslContext(false).getClientSslContext();
        assertNotNull(ctx);
        assertTrue(ctx.isClient());
    }

    public void testServerSslContextIsCached() throws Exception {
        DefaultSslContextProvider p = providerWithRealSslContext(false);
        assertSame(p.getServerSslContext(), p.getServerSslContext());
    }

    public void testClientSslContextIsCached() throws Exception {
        DefaultSslContextProvider p = providerWithRealSslContext(false);
        assertSame(p.getClientSslContext(), p.getClientSslContext());
    }

    public void testClientEngineWrappedWhenHostnameVerificationDisabled() throws Exception {
        SslContext clientCtx = providerWithRealSslContext(false).getClientSslContext();
        SSLEngine engine = clientCtx.newEngine(ByteBufAllocator.DEFAULT, "localhost", 9400);
        assertTrue(
            "engine must be AlpnAwareSSLEngineWrapper when hostname verification is off",
            engine instanceof AlpnAwareSSLEngineWrapper
        );
    }

    public void testClientEngineNotWrappedWhenHostnameVerificationEnabled() throws Exception {
        SslContext clientCtx = providerWithRealSslContext(true).getClientSslContext();
        SSLEngine engine = clientCtx.newEngine(ByteBufAllocator.DEFAULT, "localhost", 9400);
        assertFalse(engine instanceof AlpnAwareSSLEngineWrapper);
    }

    // ---- helpers ----

    private static DefaultSslContextProvider providerWithRealSslContext(boolean enforceHostnameVerification) throws Exception {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);

        SecureTransportSettingsProvider.SecureTransportParameters params = mock(
            SecureTransportSettingsProvider.SecureTransportParameters.class
        );
        when(params.clientAuth()).thenReturn(Optional.of("NONE"));
        when(params.cipherSuites()).thenReturn(List.of());

        SecureTransportSettingsProvider settingsProvider = mock(SecureTransportSettingsProvider.class);
        when(settingsProvider.buildSecureTransportContext(any())).thenReturn(Optional.of(sslContext));
        when(settingsProvider.parameters(any())).thenReturn(Optional.of(params));

        Settings settings = Settings.builder().put("transport.ssl.enforce_hostname_verification", enforceHostnameVerification).build();

        return new DefaultSslContextProvider(settingsProvider, settings);
    }
}
