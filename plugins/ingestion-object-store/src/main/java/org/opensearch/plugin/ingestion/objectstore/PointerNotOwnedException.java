/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

/**
 * Raised when a read is requested from a pointer whose object key is not among the slices this shard owns, for example a
 * stale or hand-crafted {@code reset_by_offset} pointer. Returning no records instead would leave the shard ingesting
 * nothing with no signal to the operator.
 */
final class PointerNotOwnedException extends IllegalArgumentException {

    /**
     * Creates the exception.
     *
     * @param message the detail message naming the offending pointer and the shard
     */
    public PointerNotOwnedException(String message) {
        super(message);
    }
}
