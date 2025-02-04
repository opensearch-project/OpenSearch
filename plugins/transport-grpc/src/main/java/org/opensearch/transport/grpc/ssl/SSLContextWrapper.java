/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
package org.opensearch.transport.grpc.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;

import java.security.NoSuchAlgorithmException;
import java.util.List;

import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;

/**
 * A light wrapper intended to negotiate the difference between two ssl context implementations.
 * {@link SSLContext} provided by javax.net.ssl, and
 * {@link SslContext} provided by io.grpc.
 */
public class SSLContextWrapper extends SslContext {
    private final SSLContext ctxt;
    private final boolean client;

    private static final String[] DEFAULT_SSL_PROTOCOLS = { "TLSv1.3", "TLSv1.2", "TLSv1.1" };
    private static final String[] DEFAULT_ALPN = { "h2" };

    public SSLContextWrapper(boolean isClient) throws NoSuchAlgorithmException {
        this(SSLContext.getDefault(), isClient);
    }

    public SSLContextWrapper(SSLContext javaxCtxt, boolean isClient) {
        this.ctxt = javaxCtxt;
        this.ctxt.getDefaultSSLParameters().setProtocols(DEFAULT_SSL_PROTOCOLS);
        this.client = isClient;
    }

    @Override
    public boolean isClient() {
        return client;
    }

    @Override
    public List<String> cipherSuites() {
        return List.of(ctxt.getDefaultSSLParameters().getCipherSuites());
    }

    class DefaultAPN implements ApplicationProtocolNegotiator {
        @Override
        public List<String> protocols() {
            return List.of(ctxt.getDefaultSSLParameters().getProtocols());
        }
    }

    // ApplicationProtocolNegotiator is deprecated
    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return new DefaultAPN() {
            @Override
            public List<String> protocols() {
                return List.of(DEFAULT_ALPN);
            }
        };
    }

    /**
     * javax SSLContext handles its own buffer allocation.
     * As such we can ignore the netty ByteBufAllocator when creating engines.
     */
    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator) {
        return ctxt.createSSLEngine();
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator, String s, int i) {
        return ctxt.createSSLEngine(s, i);
    }

    @Override
    public SSLSessionContext sessionContext() {
        return this.client ? ctxt.getClientSessionContext() : ctxt.getServerSessionContext();
    }
}
