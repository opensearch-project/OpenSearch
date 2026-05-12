/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.io.IOException;
import java.util.List;

/**
 * Strategy interface for Lucene merge behavior based on whether Lucene is the
 * primary or secondary data format in a composite index.
 *
 * <p>When Lucene is the <b>primary</b> format, it performs a standard merge and
 * produces a {@link RowIdMapping} that secondary formats use to align their
 * document order.
 *
 * <p>When Lucene is a <b>secondary</b> format, it receives a {@link RowIdMapping}
 * from the primary format and remaps its row ID doc values + reorders documents
 * to match the primary's merged output.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface LuceneMergeStrategy {

    /**
     * Creates the {@link MergePolicy.OneMerge} that controls how segments are merged.
     *
     * <p>Primary strategy: returns a plain {@code OneMerge} (no reader wrapping).
     * <p>Secondary strategy: returns a {@link RowIdRemappingOneMerge} that wraps readers
     * with {@link RowIdRemappingCodecReader} for row ID remapping.
     *
     * @param segments the segments to merge
     * @param rowIdMapping the row ID mapping from the primary format, or null if this is the primary
     * @return the configured OneMerge for execution
     */
    MergePolicy.OneMerge createOneMerge(List<SegmentCommitInfo> segments, RowIdMapping rowIdMapping);

    /**
     * Builds or resolves the {@link RowIdMapping} after the merge completes.
     *
     * <p>Primary strategy: builds a new mapping by reading the merged segment to determine
     * how old row IDs map to new positions in the merged output.
     * <p>Secondary strategy: passes through the input mapping (already provided by the primary).
     *
     * @param completedMerge the merge that was executed (contains merged segment info)
     * @param mergeInput the original merge input (contains input row ID mapping and segment list)
     * @return the row ID mapping for the merge result, or null if not applicable
     * @throws IOException if reading the merged segment fails
     */
    RowIdMapping buildRowIdMapping(MergePolicy.OneMerge completedMerge, MergeInput mergeInput) throws IOException;
}
