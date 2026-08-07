/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;

/**
 * Handles document deletion for a specific data format. Each deleter is paired with a
 * {@link Writer} and shares its generation.
 *
 * <p>For Parquet-only format, the deleter holds a per-generation Lucene writer for
 * indexing identity documents. For Lucene-only format, the deleter is a no-op wrapper
 * since Lucene natively tracks live docs.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Deleter extends Closeable {

    /**
     * Returns the generation number of this deleter, matching its paired writer.
     *
     * @return the generation number
     */
    long generation();

    /**
     * Deletes a document from the underlying format-specific storage.
     *
     * @param deleteInput the input containing field name, value, and generation to identify the document
     * @return the result of the delete operation
     * @throws IOException if an I/O error occurs
     */
    DeleteResult deleteDoc(DeleteInput deleteInput) throws IOException;

    Queue<String> deactivate();

    boolean recordBufferedDeletes(String id);

    boolean isActive();

    /**
     * Records a positional (row-id) delete, applied as a liveDocs-only delete during the paired
     * {@link Writer}'s flush (retained through any reorder, 1:1 with the primary format). Formats
     * without positional deletes leave this unsupported.
     *
     * @param rowId insertion row id within the paired writer's generation
     */
    default void recordPositionalDelete(long rowId) {
        throw new UnsupportedOperationException("Positional delete is not supported by this deleter");
    }
}
