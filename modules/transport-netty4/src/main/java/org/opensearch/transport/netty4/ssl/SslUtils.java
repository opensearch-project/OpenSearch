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
package org.opensearch.transport.netty4.ssl;

import org.opensearch.OpenSearchSecurityException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;

import io.netty.buffer.ByteBuf;

/**
 * @see <a href="https://github.com/opensearch-project/security/blob/d526c9f6c2a438c14db8b413148204510b9fe2e2/src/main/java/org/opensearch/security/ssl/util/TLSUtil.java">TLSUtil</a>
 */
public class SslUtils {
    private static final String[] DEFAULT_SSL_PROTOCOLS = { "TLSv1.3", "TLSv1.2", "TLSv1.1" };

    private static final int SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC = 20;
    private static final int SSL_CONTENT_TYPE_ALERT = 21;
    private static final int SSL_CONTENT_TYPE_HANDSHAKE = 22;
    private static final int SSL_CONTENT_TYPE_APPLICATION_DATA = 23;
    // CS-SUPPRESS-SINGLE: RegexpSingleline Extensions heartbeat needs special handling by security extension
    private static final int SSL_CONTENT_TYPE_EXTENSION_HEARTBEAT = 24;
    // CS-ENFORCE-SINGLE
    private static final int SSL_RECORD_HEADER_LENGTH = 5;

    private SslUtils() {

    }

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

    public static SSLEngine createDefaultClientSSLEngine() {
        try {
            final SSLEngine engine = SSLContext.getDefault().createSSLEngine();
            engine.setEnabledProtocols(DEFAULT_SSL_PROTOCOLS);
            engine.setUseClientMode(true);
            return engine;
        } catch (final NoSuchAlgorithmException ex) {
            throw new OpenSearchSecurityException("Unable to initialize default client SSL engine", ex);
        }
    }

    static boolean isTLS(ByteBuf buffer) {
        int packetLength = 0;
        int offset = buffer.readerIndex();

        // SSLv3 or TLS - Check ContentType
        boolean tls;
        switch (buffer.getUnsignedByte(offset)) {
            case SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC:
            case SSL_CONTENT_TYPE_ALERT:
            case SSL_CONTENT_TYPE_HANDSHAKE:
            case SSL_CONTENT_TYPE_APPLICATION_DATA:
                // CS-SUPPRESS-SINGLE: RegexpSingleline Extensions heartbeat needs special handling by security extension
            case SSL_CONTENT_TYPE_EXTENSION_HEARTBEAT:
                tls = true;
                break;
            // CS-ENFORCE-SINGLE
            default:
                // SSLv2 or bad data
                tls = false;
        }

        if (tls) {
            // SSLv3 or TLS - Check ProtocolVersion
            int majorVersion = buffer.getUnsignedByte(offset + 1);
            if (majorVersion == 3) {
                // SSLv3 or TLS
                packetLength = unsignedShortBE(buffer, offset + 3) + SSL_RECORD_HEADER_LENGTH;
                if (packetLength <= SSL_RECORD_HEADER_LENGTH) {
                    // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                    tls = false;
                }
            } else {
                // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                tls = false;
            }
        }

        return tls;
    }

    private static int unsignedShortBE(ByteBuf buffer, int offset) {
        return buffer.order() == ByteOrder.BIG_ENDIAN ? buffer.getUnsignedShort(offset) : buffer.getUnsignedShortLE(offset);
    }
}
