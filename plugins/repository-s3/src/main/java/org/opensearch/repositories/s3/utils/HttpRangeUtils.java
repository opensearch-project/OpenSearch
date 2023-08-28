/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.utils;

public final class HttpRangeUtils {

    /**
     * Provides a byte range string per <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-byte-ranges">RFC 9110</a>
     * @param start start position (inclusive)
     * @param end end position (inclusive)
     * @return A 'bytes=start-end' string
     */
    public static String toHttpRangeHeader(long start, long end) {
        return "bytes=" + start + "-" + end;
    }
}
