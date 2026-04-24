/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.queue.Lockable;

import java.io.Closeable;
import java.io.IOException;

/**
 * Handles document deletion for a specific data format. Each deleter is paired with a
 * {@link Writer} and shares its generation. Implements {@link Lockable} for thread-safe
 * pooling via {@link org.opensearch.common.queue.LockablePool}.
 *
 * @param <P> the document input type
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Deleter<P extends DocumentInput<?>> extends Closeable, Lockable {
    /**
     * Returns the generation number of this deleter, matching its paired writer.
     *
     * @return the generation number
     */
    long generation();

    /**
     * Deletes a document from the underlying format-specific storage.
     *
     * @param d the document input identifying the document to delete
     * @return the result of the delete operation
     * @throws IOException if an I/O error occurs
     */
    DeleteResult deleteDoc(P d) throws IOException;
}
