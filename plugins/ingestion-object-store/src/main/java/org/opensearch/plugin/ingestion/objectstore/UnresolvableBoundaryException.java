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
 * Raised by {@link BoundaryResolver} when the bytes it read show that a nominal cut cannot be resolved to a line start:
 * the object has no newline within the scan bound and so is not splittable ndjson, or it delivered fewer bytes than the
 * manifest implies and so was truncated. Both are properties of the stored data, so retrying the same ranged read
 * cannot help, and {@link ObjectStoreConsumerFactory} treats this as a configuration error. A failure to reach or read
 * the object store is still reported as a plain {@link IOException}.
 */
final class UnresolvableBoundaryException extends IOException {

    /**
     * Creates the exception.
     *
     * @param message the detail message naming the object, the offset, and why the cut cannot be resolved
     */
    UnresolvableBoundaryException(String message) {
        super(message);
    }
}
