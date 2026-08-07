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
     * <p>
     * Per-document failures (e.g., schema mismatches, partial Lucene IndexWriter advance)
     * must be returned as a {@link WriteResult.Failure} rather than thrown. The writer
     * sets its {@link #state()} to {@link WriterState#PENDING_ROLLBACK} to signal the caller that
     * {@link #rollbackTo(long)} must be invoked before any further writes. Throws are
     * reserved for unexpected errors (corrupted state, unrecoverable I/O); the engine
     * treats those as fatal and fails the shard.
     *
     * @param d the document input
     * @return {@link WriteResult.Success} on accept, {@link WriteResult.Failure} on per-doc rejection
     * @throws IOException for unexpected, fatal I/O errors only
     */
    WriteResult addDoc(P d) throws IOException;

    /**
     * Flushes the writer and returns file information.
     *
     * @param flushInput optional context for the flush operation
     * @return the file information after flush
     * @throws IOException if an I/O error occurs
     */
    FileInfos flush(FlushInput flushInput) throws IOException;

    /**
     * The generation number associated with this writer
     * @return the generation number
     */
    long generation();

    /**
     * Rolls the writer back so it holds exactly {@code rowCount} admitted documents.
     * The caller declares the target; the writer either reaches it or throws.
     *
     * <ul>
     *   <li>If the writer's current accepted count <b>equals</b> {@code rowCount}, this
     *       is a no-op — nothing was admitted past the target. State is unchanged.</li>
     *   <li>If the writer's current accepted count is <b>greater</b> than {@code rowCount},
     *       the surplus docs must be undone. Fully reversible writers (e.g., Parquet's
     *       row-count decrement) stay in {@link WriterState#ACTIVE}; writers whose internal
     *       counters cannot be reversed (e.g., Lucene's docId allocator) transition to
     *       {@link WriterState#RETIRED_FLUSHABLE} — buffered docs remain flushable but no
     *       more {@code addDoc} calls are allowed.</li>
     *   <li>If the writer's current accepted count is <b>less</b> than {@code rowCount},
     *       the writer cannot synthesize the missing rows — throw {@link IllegalStateException}.</li>
     * </ul>
     *
     * <p>A rollback that throws {@link IOException} or {@link IllegalStateException} leaves
     * the writer in an unknown state; the engine is expected to fail the shard so recovery
     * replays the translog.
     *
     * <p>Implementations must override this method — the default raises
     * {@link UnsupportedOperationException} because a writer that cannot reach a target
     * row count cannot participate in {@code CompositeWriter}'s cross-format atomicity
     * protocol.
     *
     * @param rowCount the desired number of admitted documents after this call
     * @throws IOException if the rollback fails
     * @throws IllegalStateException if the writer cannot reach {@code rowCount} (e.g.,
     *         it has accepted fewer than {@code rowCount} docs, or its current state forbids rollback)
     */
    default void rollbackTo(long rowCount) throws IOException {
        throw new UnsupportedOperationException("rollbackTo must be implemented by " + getClass().getName());
    }

    /**
     * Returns this writer's current lifecycle state. The engine inspects {@code state()}
     * after every {@link #addDoc} or {@link #rollbackTo(long)} to decide whether the writer
     * should be returned to the pool, retired with a flush, or retired without a flush.
     * <p>
     * Implementations must track and expose state honestly: returning a stale
     * {@link WriterState#ACTIVE} after a failure would leave a poisoned writer in the pool.
     *
     * @return the current state
     */
    WriterState state();

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
     * Buffers a positional (row-id) delete to be applied as a liveDocs-only delete during this
     * writer's flush (retained through any reorder, 1:1 with the primary format). Implementations
     * that support positional deletes (e.g. Lucene) override this; others leave it unsupported.
     *
     * @param rowId insertion row id (0-based position within this writer's generation) to mark deleted
     */
    default void recordPositionalDelete(long rowId) {
        throw new UnsupportedOperationException("recordPositionalDelete is not supported by this writer");
    }
}
