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
 * Coordinates per-generation delete tracking for a data format.
 *
 * @param <T> the data format type
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DeleteExecutionEngine<T extends DataFormat> extends Closeable {

    /**
     * Creates delete state for a writer generation.
     *
     * @param writer the paired writer
     * @return the deleter, or {@code null} when the writer has no delete-capable format
     */
    Deleter createDeleter(Writer<?> writer);

    /**
     * Makes buffered deletes visible to readers.
     *
     * @param refreshInput the refresh context
     * @return the refresh result
     * @throws IOException if refresh fails
     */
    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    /**
     * Returns the data format this engine handles deletes for.
     *
     * @return the data format
     */
    T getDataFormat();

    /**
     * Records a delete against the requested generation.
     *
     * @param deleteInput the document and generation
     * @param writer      the locked current writer used to determine delete capability
     * @return the delete result
     * @throws IOException if deletion fails
     */
    DeleteResult deleteDocument(DeleteInput deleteInput, Writer<?> writer) throws IOException;

    /**
     * Records that document {@code id} now lives at {@code rowId} within the active writer
     * {@code generation}. The rowId is the insertion position (0-based) within that
     * generation's segment.
     *
     * @param id         the document id
     * @param generation the writer generation the document was written to
     * @param rowId      the insertion row id within that generation
     */
    void recordWrite(String id, long generation, long rowId);

    /**
     * Retires a writer generation, removes its document locations, and applies its buffered IDs to the
     * parent writer.
     *
     * @param generation the retired writer generation
     * @return {@code true} if buffered deletes were applied
     * @throws IOException if applying deletes fails
     */
    boolean onWriterCheckedOut(long generation) throws IOException;

    /**
     * Returns estimated heap used by in-memory delete tracking.
     *
     * @return estimated bytes, or 0 when no state is tracked
     */
    default long ramBytesUsed() {
        return 0L;
    }
}
