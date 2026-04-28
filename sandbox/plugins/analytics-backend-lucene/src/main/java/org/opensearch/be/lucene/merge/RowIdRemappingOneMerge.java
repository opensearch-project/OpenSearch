/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.io.IOException;
import java.util.List;

/**
 * A custom {@link MergePolicy.OneMerge} that wraps each segment's {@link CodecReader}
 * with a {@link RowIdRemappingCodecReader} during the merge process.
 *
 * <p>The wrapped reader remaps {@code ___row_id} doc values so the merged segment stores
 * the new global row IDs. Document ordering is handled by the {@link RowIdRemappingSortField}
 * on the writer's IndexSort — {@code MultiSorter} reads the original (unwrapped) values
 * via the sort field's {@code getIndexSorter()} and builds DocMaps for reordering.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class RowIdRemappingOneMerge extends MergePolicy.OneMerge {

    private final RowIdMapping rowIdMapping;
    private int nextRowIdOffset;

    RowIdRemappingOneMerge(List<SegmentCommitInfo> segments, RowIdMapping rowIdMapping) {
        super(segments);
        this.rowIdMapping = rowIdMapping;
        this.nextRowIdOffset = 0;
    }

    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
        CodecReader wrapped = super.wrapForMerge(reader);
        long generation = resolveGeneration(wrapped);
        int offset = nextRowIdOffset;
        nextRowIdOffset += wrapped.maxDoc();
        return new RowIdRemappingCodecReader(wrapped, rowIdMapping, generation, offset);
    }

    private long resolveGeneration(CodecReader reader) {
        if (reader instanceof SegmentReader segmentReader) {
            SegmentCommitInfo sci = segmentReader.getSegmentInfo();
            String genAttr = sci.info.getAttribute(LuceneMerger.WRITER_GENERATION_ATTR);
            if (genAttr != null) {
                return Long.parseLong(genAttr);
            }
        }
        throw new IllegalStateException(
            "Cannot resolve writer generation for reader: "
                + reader.getClass().getName()
                + ". Ensure segments have the '"
                + LuceneMerger.WRITER_GENERATION_ATTR
                + "' attribute."
        );
    }
}
