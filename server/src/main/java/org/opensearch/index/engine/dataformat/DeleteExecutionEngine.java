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
 * Engine for executing delete operations for a specific data format.
 * Each deleter is paired with a writer and shares its generation, enabling
 * format-specific delete tracking (e.g., live-doc bitsets for Parquet files).
 *
 * <p>A single universal implementation handles all data formats by internally
 * using a {@code LuceneIndexingExecutionEngine} + {@code LuceneCommitter} for
 * durable delete tracking. The engine decides at runtime whether to create its
 * own Lucene infrastructure (Parquet-only) or reuse an existing one (composite/Lucene).
 *
 * @param <T> the data format type
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DeleteExecutionEngine<T extends DataFormat> extends Closeable {

    /**
     * Creates a new deleter paired with the given writer.
     * The deleter tracks deletes for documents managed by this writer.
     *
     * @param writer the writer this deleter is paired with
     * @return a new deleter instance
     */
    Deleter createDeleter(Writer<?> writer);

    /**
     * Refreshes delete state, making buffered deletes visible to readers.
     * For Parquet-only format, this incorporates per-gen Lucene segments into
     * the parent writer and builds delete bitmaps.
     *
     * @param refreshInput the refresh configuration
     * @return the result of the refresh operation
     * @throws IOException if an I/O error occurs during refresh
     */
    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    /**
     * Returns the data format this engine handles deletes for.
     *
     * @return the data format
     */
    T getDataFormat();

    /**
     * Deletes a document by looking up the deleter for the generation specified
     * in the input and delegating the delete operation.
     *
     * @param deleteInput the input containing field name, value, and generation
     * @return the result of the delete operation
     * @throws IOException if an I/O error occurs during deletion
     */
    DeleteResult deleteDocument(DeleteInput deleteInput) throws IOException;
}
