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
package org.opensearch.http.reactor.netty4.ssl;

import org.opensearch.OpenSearchSecurityException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.security.NoSuchAlgorithmException;

/**
 * Helper class for creating default SSL engines
 */
public class SslUtils {
    private static final String[] DEFAULT_SSL_PROTOCOLS = { "TLSv1.3", "TLSv1.2", "TLSv1.1" };

    private SslUtils() {}

    /**
     * Creates default server {@link SSLEngine} instance
     * @return default server {@link SSLEngine} instance
     */
    public static SSLEngine createDefaultServerSSLEngine() {
        try {
            final SSLEngine engine = SSLContext.getDefault().createSSLEngine();
            engine.setEnabledProtocols(DEFAULT_SSL_PROTOCOLS);
            engine.setUseClientMode(false);
            return engine;
        } catch (final NoSuchAlgorithmException ex) {
            throw new OpenSearchSecurityException("Unable to initialize default server SSL engine", ex);
        }
    }
}
