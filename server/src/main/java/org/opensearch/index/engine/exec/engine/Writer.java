/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Optional;

/**
 * Writer interface for document operations.
 */
@ExperimentalApi
public interface Writer<P extends DocumentInput<?>> {
    /**
     * Adds a document.
     * @param d the document input
     * @return the write result
     * @throws IOException if an I/O error occurs
     */
    WriteResult addDoc(P d) throws IOException;

    /**
     * Flushes the writer.
     * @return the file metadata
     * @throws IOException if an I/O error occurs
     */
    FileMetadata flush() throws IOException;

    /**
     * Syncs the writer.
     * @throws IOException if an I/O error occurs
     */
    void sync() throws IOException;

    /**
     * Closes the writer.
     * @throws IOException if an I/O error occurs
     */
    void close() throws IOException;

    /**
     * Gets the metadata.
     * @return the optional file metadata
     */
    Optional<FileMetadata> getMetadata();

    /**
     * Creates a new document input.
     * @return the document input
     */
    P newDocumentInput();
}
