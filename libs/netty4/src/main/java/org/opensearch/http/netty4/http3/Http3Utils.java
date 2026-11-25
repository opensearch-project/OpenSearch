/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4.http3;

import org.opensearch.common.crypto.FipsApprovedOnlyMode;

import io.netty.handler.codec.quic.Quic;

/**
 * Adapted from reactor.netty.http.internal.Http3 class
 */
public final class Http3Utils {
    static final boolean isHttp3Available;

    static {
        boolean http3;
        try {
            Class.forName("io.netty.handler.codec.http3.Http3");
            http3 = true;
        } catch (Throwable t) {
            http3 = false;
        }
        // Quic codec (which is used by HTTP/3 implementation) is provided by the
        // native library and may not be available on all platforms (even if HTTP/3
        // codec is present).
        // QUIC encrypts with the BoringSSL library that the native codec bundles, not with a Java security provider. In
        // BouncyCastle FIPS approved-only mode that would bypass the validated module, so HTTP/3 is not offered then.
        isHttp3Available = http3 && Quic.isAvailable() && FipsApprovedOnlyMode.isActive() == false;
    }

    private Http3Utils() {

    }

    /**
     * Check if the current runtime supports HTTP/3, by verifying if {@code io.netty:netty-codec-native-quic} is on the classpath.
     * In BouncyCastle FIPS approved-only mode HTTP/3 is never available, because QUIC does not use the FIPS provider.
     *
     * @return true if {@code io.netty:netty-codec-native-quic} is available and the JVM is not in FIPS approved-only mode
     */
    public static boolean isHttp3Available() {
        return isHttp3Available;
    }

}
