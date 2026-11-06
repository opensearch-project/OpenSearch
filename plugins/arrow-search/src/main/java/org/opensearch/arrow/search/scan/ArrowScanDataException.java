/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

/**
 * Shard data that a scan cannot stream, such as several values in a scalar column, a row larger than the maximum row size,
 * or a mapping that does not fit the column. Every copy of the shard holds the same data, so the failure is a bad request
 * and is never retried.
 */
public final class ArrowScanDataException extends IllegalArgumentException {

    /**
     * Creates the exception.
     *
     * @param message what the scan cannot stream
     */
    public ArrowScanDataException(String message) {
        super(message);
    }
}
