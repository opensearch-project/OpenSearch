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

import java.util.List;

/**
 * Merge strategy for when Lucene is the <b>primary</b> data format in a composite index.
 *
 * <p>As the primary format, Lucene performs a standard merge (no row ID remapping on input)
 * and produces a {@link RowIdMapping} that secondary formats use to align their document
 * order with the merged output.
 *
 * <p>The mapping is built after the merge completes by reading the merged segment to
 * determine how documents from each source generation were reordered.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PrimaryLuceneMergeStrategy implements LuceneMergeStrategy {

    @Override
    public MergePolicy.OneMerge createOneMerge(List<SegmentCommitInfo> segments, RowIdMapping rowIdMapping) {
        throw new UnsupportedOperationException("Primary Lucene merge strategy is not yet implemented");
    }

    @Override
    public RowIdMapping buildRowIdMapping(MergePolicy.OneMerge completedMerge, MergeInput mergeInput) {
        throw new UnsupportedOperationException("Primary Lucene merge strategy is not yet implemented");
    }
}
