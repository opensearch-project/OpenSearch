/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

/**
 * A reloadable implementation of io.grpc.SslContext.
 * This object delegates its functionality to an SSLEngine as retrieved from SecureAuxTransportSettingsProvider::buildSecureAuxServerEngine.
 * Delegating functionality to buildSecureAuxServerEngine take immediate advantage of cert/key updates.
 * This additionally means our SSLEngine is volatile and may change between any two function calls.
 * In the worst case the information provided during the initial handshake such as supported ciphers is stale by the time
 * we create and return the newEngine, resulting in a failed connection.
 */
public class ReloadableSecureAuxTransportSslContext extends SslContext {
    private static final String[] DEFAULT_SSL_PROTOCOLS = { "TLSv1.3", "TLSv1.2", "TLSv1.1" };
    private static final String[] HTTP2_ALPN = { "h2" };
    private final SecureAuxTransportSettingsProvider provider;
    private final boolean isClient;

    /**
     * Create and SSLEngine from the default provider if one is set.
     * @return default javax.net.ssl.SSLEngine instance.
     */
    private SSLEngine getDefaultServerSSLEngine() {
        try {
            final SSLEngine engine = SSLContext.getDefault().createSSLEngine();
            engine.setEnabledProtocols(DEFAULT_SSL_PROTOCOLS);
            return engine;
        } catch (final NoSuchAlgorithmException ex) {
            throw new OpenSearchSecurityException("Unable to initialize default server SSL engine", ex);
        }
    }

    /**
     * Initializes a new ReloadableSecureAuxTransportSslContext.
     * @param provider creation of new SSLEngine instances is delegated to this object.
     * @param isClient determines if handshake is negotiated in client or server mode.
     */
    public ReloadableSecureAuxTransportSslContext(SecureAuxTransportSettingsProvider provider, boolean isClient) {
        super();
        this.provider = provider;
        this.isClient = isClient;
    }

    /**
     * Create a new SSLEngine instance to handle TLS for a connection.
     * @param byteBufAllocator provider interface does not allow us to leverage an allocator so this param is not used.
     * @return new SSLEngine instance.
     */
    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator) {
        SSLEngine sslEngine;
        try {
            sslEngine = provider.buildSecureAuxServerEngine()
                .orElseGet(this::getDefaultServerSSLEngine);
            SSLParameters params = sslEngine.getSSLParameters();
            params.setApplicationProtocols(HTTP2_ALPN); // io.grpc.SslContext -> gRPC -> HTTP2
            sslEngine.setSSLParameters(params);
            sslEngine.setUseClientMode(isClient);
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
        return sslEngine;
    }

    /**
     * Create a new SSLEngine instance to handle TLS for a connection.
     * @param byteBufAllocator provider interface does not allow us to leverage an allocator so this param is not used.
     * @param s host hint is unused since we do not control the underlying ssl context and instead delegate to buildSecureAuxServerEngine.
     * @param i port hint - see above.
     * @return new SSLEngine instance.
     */
    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator, String s, int i) {
        // host/port are hints for an internal session reuse strategy and can be ignored safely
        return newEngine(byteBufAllocator);
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
        return Arrays.asList(newEngine(null).getEnabledCipherSuites());
    }

    /**
     * Deprecated.
     * @return HTTP2 requires "h2" be specified in ALPN.
     */
    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return () -> List.of(HTTP2_ALPN);
    }

    /**
     * Fetch the session context interface for the current underlying ssl context.
     * Sessions are invalidated on reload of the underlying ssl context.
     * @return SSLSessionContext interface.
     */
    @Override
    public SSLSessionContext sessionContext() {
        return newEngine(null).getSession().getSessionContext();
    }
}
