/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Interface for writing documents to a data format.
 *
 * @param <P> the type of document input
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Writer<P extends DocumentInput<?>> {

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
     * Closes the writer and releases resources.
     */
    void close();
}
