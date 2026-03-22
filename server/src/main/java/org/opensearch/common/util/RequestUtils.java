/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.UUIDs;

/**
 * Common utility methods for request handling.
 *
 * @opensearch.internal
 */
public final class RequestUtils {

    private RequestUtils() {}

    /**
     * Generates a new ID field for new documents.
     */
    public static String generateID() {
        return UUIDs.base64UUID();
    }

    /**
     * Validate whether X-Request-id is valid or not.
     */
    public static void validateRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("X-Request-Id should not be null or empty");
        }

        if (requestId.length() != 32) {
            throw new IllegalArgumentException("Invalid X-Request-Id passed. Should be 32 hexadecimal characters: " + requestId);
        }

        for (int i = 0; i < requestId.length(); i++) {
            char c = requestId.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                throw new IllegalArgumentException("Invalid X-Request-Id passed: " + requestId);
            }
        }
    }

}
