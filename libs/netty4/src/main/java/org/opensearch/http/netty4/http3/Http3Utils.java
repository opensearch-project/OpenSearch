/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4.http3;

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
        isHttp3Available = http3;
    }

    private Http3Utils() {

    }

    /**
     * Check if the current runtime supports HTTP/3, by verifying if {@code io.netty:netty-codec-native-quic} is on the classpath.
     *
     * @return true if {@code io.netty:netty-codec-native-quic} is available
     */
    public static boolean isHttp3Available() {
        return isHttp3Available;
    }

}
