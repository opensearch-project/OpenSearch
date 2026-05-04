/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.index.Term;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.queue.Lockable;

import java.io.Closeable;
import java.io.IOException;

/**
 * Handles document deletion for a specific data format. Each deleter is paired with a
 * {@link Writer} and shares its generation. Implements {@link Lockable} for thread-safe
 * pooling via {@link org.opensearch.common.queue.LockablePool}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Deleter extends Closeable, Lockable {
    /**
     * Returns the generation number of this deleter, matching its paired writer.
     *
     * @return the generation number
     */
    long generation();

    /**
     * Deletes a document from the underlying format-specific storage.
     *
     * @param uid the term identifying the document in lucene
     * @param rowId the row ID of the document for parquet
     * @return the result of the delete operation
     * @throws IOException if an I/O error occurs
     */
    DeleteResult deleteDoc(Term uid, Long rowId) throws IOException;
}
