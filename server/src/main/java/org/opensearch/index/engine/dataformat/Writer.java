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
import java.util.Optional;

/**
 * Interface for writing documents to a data format.
 *
 * @param <P> the type of document input
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Writer<P extends DocumentInput<?>> extends Closeable {

    /**
     * Adds a document to the writer.
     *
     * @param d the document input
     * @return the write result
     * @throws IOException if an I/O error occurs
     */
    WriteResult addDoc(P d) throws IOException;

    /**
     * Flushes the writer and returns file information.
     *
     * @return the file information after flush
     * @throws IOException if an I/O error occurs
     */
    FileInfos flush() throws IOException;

    /**
     * Synchronizes the writer to ensure data is persisted.
     *
     * @throws IOException if an I/O error occurs
     */
    void sync() throws IOException;

    /**
     * The generation number associated with this writer
     * @return the generation number
     */
    long generation();

    /**
     * Returns the underlying writer for the specified data format name.
     * Composite writers override this to return the format-specific delegate.
     * Simple writers return themselves if the format matches.
     *
     * @param formatName the name of the data format to look up
     * @return an optional containing the writer for the given format, or empty if not found
     */
    default Optional<Writer<?>> getWriterForFormat(String formatName) {
        return Optional.empty();
    }

    /**
     * Deletes a document identified by the given delete input.
     * Implementations that support direct deletion should override this method.
     *
     * @param deleteInput the input containing field name, value, and generation to identify the document
     * @return the result of the delete operation
     * @throws IOException if an I/O error occurs during deletion
     */
    default DeleteResult deleteDocument(DeleteInput deleteInput) throws IOException {
        throw new UnsupportedOperationException("deleteDocument is not supported by this writer");
    }

    /**
     * Whether this writer's schema can still evolve.
     * Formats that handle schema evolution natively (e.g., Lucene) can always return true.
     *
     * @return true if the schema is mutable
     */
    boolean isSchemaMutable();

    /**
     * The current mapping version this writer is associated with.
     *
     * @return the mapping version
     */
    long mappingVersion();

    /**
     * Update the mapping version on a writer. Implementations must ignore
     * the call if {@code newVersion} is less than or equal to the current version
     * (i.e., mapping version must only move forward).
     *
     * @param newVersion the new mapping version
     */
    void updateMappingVersion(long newVersion);
}
