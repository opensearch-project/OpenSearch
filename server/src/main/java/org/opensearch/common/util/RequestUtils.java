/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.UUIDs;

import java.util.regex.Pattern;

/**
 * Common utility methods for request handling.
 *
 * @opensearch.internal
 */
public final class RequestUtils {

    private RequestUtils() {}

    /** Matches ASCII control characters (0x00-0x1F and 0x7F). */
    private static final Pattern CONTROL_CHARS = Pattern.compile("\\p{Cntrl}");

    /**
     * Removes control characters from a client-provided header value so it is safe to emit in
     * single-line log output and response headers. A {@code null} input is returned unchanged;
     * well-formed values (UUIDs, hexadecimal or alphanumeric identifiers) are unaffected.
     */
    public static String sanitizeHeaderValue(String value) {
        if (value == null) {
            return null;
        }
        return CONTROL_CHARS.matcher(value).replaceAll("");
    }

    /**
     * Generates a new ID field for new documents.
     */
    public static String generateID() {
        return UUIDs.base64UUID();
    }

    /**
     * Validate whether X-Request-Id is valid or not.
     * The request ID must be non-empty and not exceed the configured maximum length.
     */
    public static void validateRequestId(String requestId, int maxLength) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("X-Request-Id should not be null or empty");
        }
        if (requestId.length() > maxLength) {
            throw new IllegalArgumentException(
                "X-Request-Id length [" + requestId.length() + "] exceeds maximum allowed length [" + maxLength + "]"
            );
        }
    }

}
