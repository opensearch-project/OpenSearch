/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

/**
 * Raised when the repository named by the {@code repository} parameter is registered but cannot back an ingestion
 * source, because it is not a blob-store repository or is not registered read-only. Retrying cannot help: the
 * registered repository stays unusable until it is re-registered or the parameter points at another one, so
 * {@link ObjectStoreConsumerFactory} treats it as a configuration error. A repository that is not registered at all is
 * not reported this way, since an administrator may still register it under the same name.
 */
final class UnusableRepositoryException extends IllegalArgumentException {

    /**
     * Creates the exception.
     *
     * @param message the detail message naming the repository and why it cannot be used
     */
    UnusableRepositoryException(String message) {
        super(message);
    }
}
