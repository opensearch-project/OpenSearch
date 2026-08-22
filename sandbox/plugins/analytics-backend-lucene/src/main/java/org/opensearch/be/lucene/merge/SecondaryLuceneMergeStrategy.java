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
import java.util.Map;

/**
 * Merge strategy for when Lucene is a <b>secondary</b> data format in a composite index.
 *
 * <p>As a secondary format, Lucene receives per-generation {@link RowIdMapping}s from the
 * primary format and must:
 * <ol>
 *   <li>Remap row ID doc values to the new global IDs (via {@link RowIdRemappingCodecReader})</li>
 *   <li>Reorder documents to match the primary format's merged output (via IndexSort on the
 *       row ID field)</li>
 * </ol>
 *
 * <p>This strategy creates a {@link RowIdRemappingOneMerge} that wraps each segment's
 * {@link org.apache.lucene.index.CodecReader} during the merge process. The
 * {@code buildRowIdMappings} method passes through the input mappings since the primary
 * format is the authority on document ordering.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SecondaryLuceneMergeStrategy implements LuceneMergeStrategy {

    @Override
    public MergePolicy.OneMerge createOneMerge(
        List<SegmentCommitInfo> segments,
        Map<Long, RowIdMapping> rowIdMappings,
        long outputWriterGeneration
    ) {
        return new RowIdRemappingOneMerge(segments, rowIdMappings, outputWriterGeneration);
    }

    @Override
    public Map<Long, RowIdMapping> buildRowIdMappings(MergePolicy.OneMerge completedMerge, MergeInput mergeInput) {
        // Secondary format passes through the mappings from the primary — it does not produce its own.
        return mergeInput.rowIdMappings();
    }
}
