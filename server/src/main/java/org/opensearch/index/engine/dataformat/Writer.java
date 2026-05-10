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
     * Rolls back the last document added to this writer.
     * Only valid immediately after {@link #addDoc} — cannot skip or rollback earlier docs.
     * Called by composite writers when a sibling format fails after this writer already accepted the doc.
     *
     * @throws IOException if the rollback fails
     * @throws IllegalStateException if no document has been added or last doc was already rolled back
     */
    default void rollbackLastDoc() throws IOException {}

    /**
     * Returns whether this writer has been aborted.
     *
     * @return {@code true} if aborted, {@code false} otherwise
     */
    default boolean isAborted() {
        return false;
    }
}
