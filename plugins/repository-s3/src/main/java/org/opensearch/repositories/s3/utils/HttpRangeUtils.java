/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.utils;

import software.amazon.awssdk.core.exception.SdkException;

import org.opensearch.common.collect.Tuple;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HttpRangeUtils {

    private static final Pattern RANGE_PATTERN = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$");

    // TODO: Find an alternative for parsing the header
    public static Tuple<Long, Long> fromHttpRangeHeader(String headerValue) {
        Matcher matcher = RANGE_PATTERN.matcher(headerValue);
        if (!matcher.find()) {
            throw SdkException.create("Regex match for Content-Range header {" + headerValue + "} failed", new RuntimeException());
        }
        return new Tuple<>(Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)));
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
