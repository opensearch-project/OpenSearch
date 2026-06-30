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
     * Optional pre-merge hook. Implementations that need to freeze format-internal state
     * (e.g. a Lucene secondary capturing its live-docs snapshot via
     * {@link org.apache.lucene.index.MergeIndexWriter#prepareMerge}) do so here and return a
     * per-segment {@link LiveDocs} reflecting that frozen view, so the primary-format merger
     * drops the same rows the secondary will physically drop.
     *
     * <p>If the returned LiveDocs is non-empty, the caller must subsequently invoke
     * {@link #merge} or {@link #abortPreparedMerge} with the same generation to release any
     * resources taken here. Default returns {@link LiveDocs#ALL_ALIVE} (no-op).
     */
    default LiveDocs prepareMerge(MergeInput mergeInput) throws IOException {
        return LiveDocs.ALL_ALIVE;
    }

    /**
     * Releases resources acquired by {@link #prepareMerge} when the subsequent
     * {@link #merge} call won't happen. Default no-op.
     */
    default void abortPreparedMerge(MergeInput mergeInput) throws IOException {
        // no-op
    }
}
