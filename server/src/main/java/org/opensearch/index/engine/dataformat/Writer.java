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
import java.util.Optional;

/**
 * Interface for writing documents to a data format.
 *
 * @param <P> the type of document input
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Writer<P extends DocumentInput<?>> extends Closeable, Lockable {

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
     * Finds a child writer whose data format matches the given format name.
     * Composite writers override this to search their delegates.
     *
     * @param formatName the data format name to match (e.g. "lucene")
     * @return the matching writer, or empty if not found or not composite
     */
    default Optional<Writer<?>> findWriterByFormat(String formatName) {
        return Optional.empty();
    }

    /**
     * Deletes all documents matching the given term from this writer.
     * Lucene-backed writers override this to delegate to {@code IndexWriter.deleteDocuments}.
     *
     * @param uid the term identifying the document(s) to delete
     * @throws IOException if a low-level I/O error occurs
     */
    default void deleteDocument(Term uid) throws IOException {
        throw new UnsupportedOperationException("deleteDocument not supported by " + getClass().getName());
    }
}
