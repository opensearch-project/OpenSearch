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
import java.util.concurrent.atomic.AtomicLong;

/**
 * A decorator around {@link Writer} that assigns a monotonically increasing row ID
 * to each document before delegating to the underlying writer.
 *
 * <p>Row IDs are the cross-format correlation key: when a document is written to
 * multiple data formats (e.g., Parquet for columnar storage and Lucene for inverted
 * indices), the row ID ensures that the same logical document occupies the same
 * position in every format's segment. This 1:1 offset correspondence is critical
 * for merge operations that must rewrite row IDs consistently across formats.
 *
 * <p>Each {@code RowIdAwareWriter} instance maintains its own counter starting at 0,
 * producing sequential IDs within the scope of a single writer generation. The counter
 * is tied to the writer's lifecycle — when the writer is closed and garbage collected,
 * the counter is reclaimed with it, avoiding any long-lived map or registry.
 *
 * <p>This decorator is created by {@link org.opensearch.index.engine.DataFormatAwareEngine}
 * when it wraps each writer from the {@link IndexingExecutionEngine}. The engine calls
 * {@link #addDoc} which sets the row ID on the {@link DocumentInput} and then delegates
 * to the underlying writer's {@code addDoc}.
 *
 * @param <P> the document input type accepted by the underlying writer
 * @opensearch.experimental
 */
@ExperimentalApi
public class RowIdAwareWriter<P extends DocumentInput<?>> implements Writer<P> {

    private final Writer<P> delegate;
    private final AtomicLong rowIdCounter;

    /**
     * Creates a new row-ID-aware writer wrapping the given delegate.
     *
     * @param delegate the underlying writer to delegate all operations to
     */
    public RowIdAwareWriter(Writer<P> delegate) {
        this.delegate = delegate;
        this.rowIdCounter = new AtomicLong(0);
    }

    /**
     * Assigns a sequential row ID to the document input, then delegates to the
     * underlying writer. The row ID is set via {@link DocumentInput#setRowId}
     * using the standard {@link DocumentInput#ROW_ID_FIELD} field name.
     *
     * @param d the document input to write
     * @return the write result from the underlying writer
     * @throws IOException if the underlying write fails
     */
    @Override
    public WriteResult addDoc(P d) throws IOException {
        d.setRowId(DocumentInput.ROW_ID_FIELD, rowIdCounter.getAndIncrement());
        return delegate.addDoc(d);
    }

    /** {@inheritDoc} Delegates to the underlying writer. */
    @Override
    public FileInfos flush() throws IOException {
        return delegate.flush();
    }

    /** {@inheritDoc} Delegates to the underlying writer. */
    @Override
    public void sync() throws IOException {
        delegate.sync();
    }

    /** {@inheritDoc} Returns the generation of the underlying writer. */
    @Override
    public long generation() {
        return delegate.generation();
    }

    /** {@inheritDoc} Closes the underlying writer. */
    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
