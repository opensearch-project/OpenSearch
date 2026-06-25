/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Lifecycle state of a {@link Writer}. Inspected by the engine after every write or
 * rollback to decide whether the writer can keep accepting docs, must be retired with
 * a final flush, or must be discarded.
 *
 * <p>Allowed transitions (every other transition is a contract violation and must
 * throw {@code IllegalStateException}):
 * <pre>
 *   ACTIVE ──────────addDoc(success)─────────▶ ACTIVE
 *   ACTIVE ──────────addDoc(Failure)─────────▶ PENDING_ROLLBACK
 *   ACTIVE ──────────rollbackTo (sibling)────▶ ACTIVE             (fully reversible, e.g. Parquet)
 *   ACTIVE ──────────rollbackTo (sibling)────▶ RETIRED_FLUSHABLE  (counters can't reverse, e.g. Lucene)
 *   PENDING_ROLLBACK ─rollbackTo─────────────▶ ACTIVE             (fully reversible)
 *   PENDING_ROLLBACK ─rollbackTo─────────────▶ RETIRED_FLUSHABLE  (counters can't reverse)
 *   RETIRED_FLUSHABLE ─flush+close (engine)──▶ CLOSED
 *   any state ────close (engine teardown)────▶ CLOSED
 * </pre>
 *
 * <p>None of these states fail the shard — the engine only fails on tragic events
 * (corruption, translog/committer exceptions) via {@code failEngine}, independent of
 * {@code WriterState}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum WriterState {
    /**
     * Writer is accepting new documents.
     *
     * <p>Engine action: release back to the pool; subsequent {@code addDoc} calls are allowed.
     */
    ACTIVE,

    /**
     * Writer's last {@code addDoc} returned a {@link org.opensearch.index.engine.dataformat.WriteResult.Failure}
     * and the caller must invoke {@link Writer#rollbackTo(long)} to reconcile the writer.
     * Post-rollback the writer transitions to {@link #ACTIVE} (fully reversible, e.g. Parquet)
     * or {@link #RETIRED_FLUSHABLE} (counters can't reverse, e.g. Lucene).
     */
    PENDING_ROLLBACK,
    /**
     * Writer can no longer accept documents but holds N-1 consistent docs worth
     * flushing before close. Reached after a successful {@code rollbackTo} on a
     * writer whose counters cannot be reversed — e.g., Lucene's soft-delete-based
     * rollback, where the docId allocator advances even when the doc is undone.
     *
     * <p>Engine action: checkout from pool, call {@code flush}, queue the resulting
     * segment in {@code pendingSegments} for the next refresh, then close the writer.
     */
    RETIRED_FLUSHABLE,

    /**
     * Terminal state set after the engine has closed the writer. No further operations
     * are valid on the writer.
     */
    CLOSED
}
