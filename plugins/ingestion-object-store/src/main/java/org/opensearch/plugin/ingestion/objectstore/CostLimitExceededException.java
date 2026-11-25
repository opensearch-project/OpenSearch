/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import java.io.IOException;

/**
 * Raised when a configured cost cap ({@code max_list_requests}, {@code max_get_requests}, or {@code max_bytes})
 * would be exceeded by an operation against the backing object store. Surfaced as a consumer error so ingestion
 * halts deterministically rather than issuing unbounded, unbilled traffic.
 */
final class CostLimitExceededException extends IOException {

    /**
     * Creates the exception.
     *
     * @param message the detail message naming the exceeded cap and the offending counts
     */
    public CostLimitExceededException(String message) {
        super(message);
    }
}
