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
