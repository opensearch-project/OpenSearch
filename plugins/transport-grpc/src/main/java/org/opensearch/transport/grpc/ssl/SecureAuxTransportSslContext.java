/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.TrustManagerFactory;

import java.util.List;
import java.util.Optional;

import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;

import static io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.NONE;
import static io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.OPTIONAL;
import static io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.REQUIRE;
import static io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider.JDK;
import static io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider.OPENSSL;
import static io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider.OPENSSL_REFCNT;

/**
 * An io.grpc.SslContext which builds and delegates functionality to an internal delegate.
 * As this ssl context is provided for aux transports it operates in server mode always.
 * TODO: Currently a light SslContext wrapper - hot swap functionality will be added here.
 */
public class SecureAuxTransportSslContext extends SslContext {
    private final SslContext sslContext;

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
     * Initializes a new SecureAuxTransportSslContext.
     * @param provider source of SecureAuxTransportParameters required to build an SslContext.
     */
    public SecureAuxTransportSslContext(SecureAuxTransportSettingsProvider provider) {
        Optional<SecureAuxTransportSettingsProvider.SecureAuxTransportParameters> params = provider.parameters();
        if (params.isEmpty()) {
            throw new OpenSearchSecurityException(
                "Aux transport ssl context failed to initialize. Secure settings provider not found."
            );
        }
        try {
            this.sslContext = buildContext(params.get());
        } catch (SSLException e) {
            throw new OpenSearchSecurityException("Unable to build io.grpc.SslContext from secure settings", e);
        }
    }

    /**
     * @param p fields necessary to construct an SslContext.
     * @return new SslContext.
     */
    private SslContext buildContext(SecureAuxTransportSettingsProvider.SecureAuxTransportParameters p) throws SSLException {
        if (p.keyManagerFactory().isEmpty()) {
            throw new OpenSearchSecurityException(
                "Aux transport ssl context failed to initialize. No keystore provided."
            );
        }
        if (p.sslProvider().isEmpty()) {
            throw new OpenSearchSecurityException(
                "Aux transport ssl context failed to initialize. Ssl provider not found."
            );
        }
        if (p.clientAuth().isEmpty()) {
            throw new OpenSearchSecurityException(
                "Aux transport ssl context failed to initialize. No client auth mode configured."
            );
        }
        if (p.trustManagerFactory().isEmpty()) {
            throw new OpenSearchSecurityException(
                "Aux transport ssl context failed to initialize. No truststore provided."
            );
        }
        SslContextBuilder builder = SslContextBuilder.forServer(p.keyManagerFactory().get())
            .sslProvider(providerHelper(p.sslProvider().get()))
            .protocols(p.protocols())
            .ciphers(p.cipherSuites());
        builder.applicationProtocolConfig(
            new ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                ApplicationProtocolNames.HTTP_2
            )
        );
        builder.clientAuth(clientAuthHelper(p.clientAuth().get()));
        builder.trustManager(p.trustManagerFactory().get());
        return builder.build();
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
     * @return server only context - always false.
     */
    @Override
    public boolean isClient() {
        return false;
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
