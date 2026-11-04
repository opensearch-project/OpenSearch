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
 * Interface for merging multiple writer file sets into a single merged result.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Merger {
    /**
     * Merges a list of writer file sets into a single merged result.
     *
     * @param mergeInput input containing files to merge, and any instructions about how to execute the merge.
     * @return merge result containing row ID mapping and merged file metadata
     */
    MergeResult merge(MergeInput mergeInput) throws IOException;

    /**
     * Whether this format owns delete state and can freeze it for the duration of a merge via
     * {@link #prepareMerge}. In a composite at most one format may return {@code true}; the
     * composite validates this when it is built and fails fast otherwise, so a second producer
     * cannot be silently ignored.
     */
    default boolean providesMergeLiveDocs() {
        return false;
    }

    /**
     * Phase 1 of a two-phase merge. A format that {@link #providesMergeLiveDocs() provides live
     * docs} freezes its delete state for the merge inputs here and publishes the frozen view, so
     * the primary-format merger drops exactly the rows this format will physically drop in
     * {@link #merge}. Implementations must derive the view from the merge's frozen readers, not
     * from a fresh reader, or the two formats diverge on any delete that lands in between.
     *
     * <p>The returned {@link MergePreparation} owns whatever was pinned to freeze the view. The
     * caller holds it in a try-with-resources around {@link #merge} so it is released whether or
     * not the merge runs; {@link MergePreparation#close()} is a no-op once {@code merge()} has
     * consumed the prepared state. Default returns {@link MergePreparation#EMPTY}.
     */
    default MergePreparation prepareMerge(MergeInput mergeInput) throws IOException {
        return MergePreparation.EMPTY;
    }
}
