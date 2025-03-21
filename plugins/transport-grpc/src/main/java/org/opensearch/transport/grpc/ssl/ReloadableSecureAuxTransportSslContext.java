/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSessionContext;

import java.util.List;

import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import static io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.NONE;
import static io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.OPTIONAL;
import static io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.REQUIRE;
import static io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider.JDK;
import static io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider.OPENSSL;
import static io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider.OPENSSL_REFCNT;

/**
 * A reloadable implementation of io.grpc.SslContext.
 * Delegates to an internal volatile SslContext built from SecureAuxTransportSettingsProvider.
 * On each use of this SslContext we will determine if the previous context is out of date and update if possible.
 * TODO: Each operation should check if the params are out of date with a dirty bit in the SecureAuxTransportParameters.
 */
public class ReloadableSecureAuxTransportSslContext extends SslContext {
    private final SecureAuxTransportSettingsProvider provider;
    private final boolean isClient;

    private volatile SecureAuxTransportSettingsProvider.SecureAuxTransportParameters params;
    private volatile SslContext sslContext;

    /**
     * Simple client auth string to enum conversion helper.
     * @param clientAuthStr client auth as string.
     * @return ClientAuth enum.
     */
    public static ClientAuth clientAuthHelper(String clientAuthStr) {
        switch (clientAuthStr) {
            case "NONE" -> {
                return NONE;
            }
            case "OPTIONAL" -> {
                return OPTIONAL;
            }
            case "REQUIRE" -> {
                return REQUIRE;
            }
            default -> throw new OpenSearchSecurityException("unsupported client auth: " + clientAuthStr);
        }
    }

    /**
     * Simple ssl provider string to enum conversion helper.
     * @param providerStr provider as string.
     * @return provider enum.
     */
    public static SslProvider providerHelper(String providerStr) {
        switch (providerStr) {
            case "JDK" -> {
                return JDK;
            }
            case "OPENSSL" -> {
                return OPENSSL;
            }
            case "OPENSSL_REFCNT" -> {
                return OPENSSL_REFCNT;
            }
            default -> throw new OpenSearchSecurityException("unsupported ssl provider: " + providerStr);
        }
    }

    /**
     * Initializes a new ReloadableSecureAuxTransportSslContext.
     * @param provider source of SecureAuxTransportParameters required to build an SslContext.
     * @param isClient determines if handshake is negotiated in client or server mode.
     */
    public ReloadableSecureAuxTransportSslContext(SecureAuxTransportSettingsProvider provider, boolean isClient) {
        this.provider = provider;
        this.isClient = isClient;
        this.params = provider.parameters().orElseThrow();
        try {
            this.sslContext = buildContext(params);
        } catch (SSLException e) {
            throw new OpenSearchSecurityException("Unable to build io.grpc.SslContext from secure settings", e);
        }
    }

    /**
     * @param p fields necessary to construct an SslContext.
     * @return new SslContext.
     */
    private SslContext buildContext(SecureAuxTransportSettingsProvider.SecureAuxTransportParameters p) throws SSLException {
        SslContextBuilder builder =
            isClient?
                SslContextBuilder.forClient():
                SslContextBuilder.forServer(p.keyManagerFactory().orElseThrow());

        builder.applicationProtocolConfig(
            new ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                ApplicationProtocolNames.HTTP_2
            )
        );

        if (!isClient) {
            builder
                .clientAuth(clientAuthHelper(p.clientAuth().orElseThrow()));
        }

        return builder
            .trustManager(p.trustManagerFactory().orElseThrow())
            .sslProvider(providerHelper(p.sslProvider().orElseThrow()))
            .protocols(p.protocols())
            .ciphers(p.cipherSuites())
            .build();
    }

    /*
      Mirror the io.grpc.netty.shaded.io.netty.handler.ssl API with our delegate.
      Note sslContext is volatile and active connections may fail if a hot swap occurs.
     */

    /**
     * Create a new SSLEngine instance to handle TLS for a connection.
     * @param byteBufAllocator netty allocator.
     * @return new SSLEngine instance.
     */
    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator) {
        return sslContext.newEngine(byteBufAllocator);
    }

    /**
     * Create a new SSLEngine instance to handle TLS for a connection.
     * @param byteBufAllocator netty allocator.
     * @param s host hint.
     * @param i port hint.
     * @return new SSLEngine instance.
     */
    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator, String s, int i) {
        return sslContext.newEngine(byteBufAllocator, s, i);
    }

    /**
     * @return is this a client or server context.
     */
    @Override
    public boolean isClient() {
        return this.isClient;
    }

    /**
     * @return supported cipher suites.
     */
    @Override
    public List<String> cipherSuites() {
        return sslContext.cipherSuites();
    }

    /**
     * Deprecated.
     * @return HTTP2 requires "h2" be specified in ALPN.
     */
    @Deprecated
    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return sslContext.applicationProtocolNegotiator();
    }

    /**
     * @return session context.
     */
    @Override
    public SSLSessionContext sessionContext() {
        return sslContext.sessionContext();
    }
}
