/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Engine for executing indexing operations for a specific data format.
 * Provides writer creation, merging, refresh, and file management capabilities.
 *
 * @param <T> the data format type
 * @param <P> the document input type
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexingExecutionEngine<T extends DataFormat, P extends DocumentInput<?>> {
    /**
     * Creates a new writer for the given writer generation.
     *
     * @param writerGeneration the writer generation number
     * @return a new writer instance
     */
    Writer<P> createWriter(long writerGeneration);

    /**
     * Returns the merger for combining writer file sets.
     *
     * @return the merger instance
     */
    Merger getMerger();

    /**
     * Performs a refresh operation to make recently written data searchable.
     *
     * @param refreshInput the input containing segments and writer files to refresh
     * @return the refresh result containing refreshed segments
     * @throws IOException if an I/O error occurs during refresh
     */
    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    /**
     * Returns the next writer generation number to be used when creating a new writer.
     * Each writer is associated with a monotonically increasing generation number
     * that uniquely identifies it within this engine's lifecycle.
     *
     * @return the next writer generation number
     */
    long getNextWriterGeneration();

    /**
     * Returns the data format handled by this engine.
     *
     * @return the data format
     */
    T getDataFormat();

    /**
     * Returns the amount of native (off-heap) memory used by this engine.
     *
     * @return native memory usage in bytes
     */
    default long getNativeBytesUsed() {
        return 0;
    }

    /**
     * Deletes the specified files grouped by directory.
     *
     * @param filesToDelete map of directory paths to collections of file names to delete
     * @throws IOException if an I/O error occurs during deletion
     */
    void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException;

    /**
     * Creates a new empty document input for this engine's data format.
     *
     * @return a new document input instance
     */
    P newDocumentInput();

    IndexStoreProvider getProvider();
}
