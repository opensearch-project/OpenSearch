/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import java.io.IOException;

/**
 * Translates a Lucene {@code docId} (within a single segment) into the Parquet <b>row position</b>
 * for that document in the segment's Parquet file.
 *
 * <p>The Parquet DocValues iterators address rows positionally. In the simplest case — a
 * freshly-refreshed, un-merged segment — Lucene {@code docId} equals the Parquet row, so the
 * {@link #IDENTITY} resolver is correct. After a merge, the composite engine reorders documents and
 * rewrites the per-document {@code __row_id__} doc value so that {@code docId} no longer equals the
 * Parquet row; the correct row must then be read from {@code __row_id__}. A resolver backed by that
 * doc value (see the leaf reader) provides the general, merge-safe translation.
 *
 * <p>Resolvers are <b>forward-only and stateful</b> when backed by a doc-values iterator: callers
 * must invoke {@link #toRowId(int)} with non-decreasing {@code docId}s, matching the ascending
 * access pattern of the codec's DocValues iterators. One resolver instance is therefore bound to one
 * codec iterator instance.
 */
@FunctionalInterface
public interface RowIdResolver {

    /**
     * Returns the Parquet row position for {@code docId}.
     *
     * @param docId the Lucene document id within the segment
     * @return the Parquet row position to read for this document
     * @throws IOException if the underlying row-id doc values cannot be read
     * @throws IllegalStateException if the document has no row-id value (a correctness invariant
     *         violation — every composite-engine document carries {@code __row_id__})
     */
    long toRowId(int docId) throws IOException;

    /** Identity mapping: {@code rowId == docId}. Correct for un-merged, single-generation segments. */
    RowIdResolver IDENTITY = docId -> docId;
}
