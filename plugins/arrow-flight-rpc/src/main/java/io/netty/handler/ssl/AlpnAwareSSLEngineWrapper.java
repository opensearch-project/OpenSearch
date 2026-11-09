/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package io.netty.handler.ssl;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import java.nio.ByteBuffer;

/**
 * {@link SSLEngine} wrapper that:
 * <ol>
 *   <li>Implements {@link ApplicationProtocolAccessor} (package-private in Netty) so that
 *       {@link SslHandler#applicationProtocol()} can read the negotiated ALPN protocol.
 *       This class must reside in the {@code io.netty.handler.ssl} package to access that
 *       interface.</li>
 *   <li>Strips {@code endpointIdentificationAlgorithm} from {@link #setSSLParameters} calls
 *       to prevent hostname verification from being re-enabled after the engine is created.</li>
 * </ol>
 */
public final class AlpnAwareSSLEngineWrapper extends SSLEngine implements ApplicationProtocolAccessor {
    private final SSLEngine d;

    public AlpnAwareSSLEngineWrapper(SSLEngine delegate) {
        super(delegate.getPeerHost(), delegate.getPeerPort());
        this.d = delegate;
    }

    @Override
    public String getNegotiatedApplicationProtocol() {
        if (d instanceof ApplicationProtocolAccessor a) {
            return a.getNegotiatedApplicationProtocol();
        }
        return d.getApplicationProtocol();
    }

    @Override
    public void setSSLParameters(SSLParameters p) {
        p.setEndpointIdentificationAlgorithm("");
        d.setSSLParameters(p);
    }

    @Override
    public SSLParameters getSSLParameters() {
        return d.getSSLParameters();
    }

    @Override
    public String getApplicationProtocol() {
        return d.getApplicationProtocol();
    }

    @Override
    public String getHandshakeApplicationProtocol() {
        return d.getHandshakeApplicationProtocol();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return d.getSupportedCipherSuites();
    }

    @Override
    public String[] getEnabledCipherSuites() {
        return d.getEnabledCipherSuites();
    }

    @Override
    public void setEnabledCipherSuites(String[] s) {
        d.setEnabledCipherSuites(s);
    }

    @Override
    public String[] getSupportedProtocols() {
        return d.getSupportedProtocols();
    }

    @Override
    public String[] getEnabledProtocols() {
        return d.getEnabledProtocols();
    }

    @Override
    public void setEnabledProtocols(String[] p) {
        d.setEnabledProtocols(p);
    }

    @Override
    public SSLSession getSession() {
        return d.getSession();
    }

    @Override
    public SSLSession getHandshakeSession() {
        return d.getHandshakeSession();
    }

    @Override
    public void beginHandshake() throws SSLException {
        d.beginHandshake();
    }

    @Override
    public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
        return d.getHandshakeStatus();
    }

    @Override
    public void setUseClientMode(boolean m) {
        d.setUseClientMode(m);
    }

    @Override
    public boolean getUseClientMode() {
        return d.getUseClientMode();
    }

    @Override
    public void setNeedClientAuth(boolean n) {
        d.setNeedClientAuth(n);
    }

    @Override
    public boolean getNeedClientAuth() {
        return d.getNeedClientAuth();
    }

    @Override
    public void setWantClientAuth(boolean w) {
        d.setWantClientAuth(w);
    }

    @Override
    public boolean getWantClientAuth() {
        return d.getWantClientAuth();
    }

    @Override
    public void setEnableSessionCreation(boolean f) {
        d.setEnableSessionCreation(f);
    }

    @Override
    public boolean getEnableSessionCreation() {
        return d.getEnableSessionCreation();
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer[] s, int o, int l, ByteBuffer dst) throws SSLException {
        return d.wrap(s, o, l, dst);
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer s, ByteBuffer[] ds, int o, int l) throws SSLException {
        return d.unwrap(s, ds, o, l);
    }

    @Override
    public Runnable getDelegatedTask() {
        return d.getDelegatedTask();
    }

    @Override
    public void closeInbound() throws SSLException {
        d.closeInbound();
    }

    @Override
    public boolean isInboundDone() {
        return d.isInboundDone();
    }

    @Override
    public void closeOutbound() {
        d.closeOutbound();
    }

    @Override
    public boolean isOutboundDone() {
        return d.isOutboundDone();
    }
}
