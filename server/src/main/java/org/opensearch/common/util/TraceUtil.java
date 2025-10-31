/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

/**
 * Util class to extract trace id from traceparent header
 */
public class TraceUtil {

    /**
     * Extract trace id from traceparent header
     * @param traceparent traceparent header value
     * @return trace id
     */
    public static String extractTraceId(String traceparent) {
        if (traceparent == null || traceparent.length() < 55) {
            throw new IllegalArgumentException("invalid traceparent");
        }
        return traceparent.split("-")[1];
    }
}
