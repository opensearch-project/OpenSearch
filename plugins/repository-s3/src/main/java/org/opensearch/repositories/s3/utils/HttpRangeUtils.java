/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.utils;

import software.amazon.awssdk.core.exception.SdkException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HttpRangeUtils {
    private static final Pattern RANGE_PATTERN = Pattern.compile("^bytes\\s+(\\d+)-\\d+[/\\d*]+$");

    /**
     * Parses the content range header string value to calculate the start (offset) of the HTTP response.
     * Tests against the RFC9110 specification of content range string.
     * Sample values: "bytes 0-10/200", "bytes 0-10/*"
     *  <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-content-range">Details here</a>
     * @param headerValue Header content range string value from the HTTP response
     * @return Start (Offset) value of the HTTP response
     */
    public static Long getStartOffsetFromRangeHeader(String headerValue) {
        Matcher matcher = RANGE_PATTERN.matcher(headerValue);
        if (!matcher.find()) {
            throw SdkException.create("Regex match for Content-Range header {" + headerValue + "} failed", new RuntimeException());
        }
        return Long.parseLong(matcher.group(1));
    }

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
