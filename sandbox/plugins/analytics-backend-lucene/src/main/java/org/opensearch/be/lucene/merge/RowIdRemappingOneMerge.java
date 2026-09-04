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
import java.util.Map;

import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * A custom {@link MergePolicy.OneMerge} that wraps each segment's {@link CodecReader}
 * with a {@link RowIdRemappingCodecReader} during the merge process.
 *
 * <p>The wrapped reader remaps row ID doc values so the merged segment stores
 * the new global row IDs. Document ordering is handled by the IndexSort (a
 * {@code SortedNumericSortField} on the row ID field) — {@code MultiSorter} reads the
 * already-remapped values and builds DocMaps for reordering.
 *
 * <p>This class resolves the writer generation for each segment and looks up the
 * corresponding per-generation {@link RowIdMapping} from the provided map. Each
 * {@link RowIdRemappingCodecReader} receives only the single mapping for its generation,
 * eliminating the need to carry generation information further downstream.
 *
 * <p>This class also stamps the {@link org.opensearch.be.lucene.index.LuceneWriter#WRITER_GENERATION_ATTRIBUTE}
 * onto the merged segment's {@link org.apache.lucene.index.SegmentInfo} via
 * {@link #setMergeInfo(SegmentCommitInfo)}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class RowIdRemappingOneMerge extends MergePolicy.OneMerge {

    private final Map<Long, RowIdMapping> rowIdMappings;
    private final long outputWriterGeneration;
    private int nextRowIdOffset;

    RowIdRemappingOneMerge(List<SegmentCommitInfo> segments, Map<Long, RowIdMapping> rowIdMappings, long outputWriterGeneration) {
        super(segments);
        this.rowIdMappings = rowIdMappings;
        this.outputWriterGeneration = outputWriterGeneration;
        this.nextRowIdOffset = 0;
    }

    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
        CodecReader wrapped = super.wrapForMerge(reader);
        long generation = resolveGeneration(wrapped);
        RowIdMapping mapping = rowIdMappings.get(generation);
        int offset = nextRowIdOffset;
        nextRowIdOffset += wrapped.maxDoc();
        return new RowIdRemappingCodecReader(wrapped, mapping, offset);
    }

    /**
     * Stamps the writer generation attribute on the merged segment so it is persisted when
     * Lucene writes the {@code .si} file.
     */
    @Override
    public void setMergeInfo(SegmentCommitInfo info) {
        super.setMergeInfo(info);
        if (info != null) {
            info.info.putAttribute(WRITER_GENERATION_ATTRIBUTE, String.valueOf(outputWriterGeneration));
        }
    }

    private long resolveGeneration(CodecReader reader) {
        if (reader instanceof SegmentReader segmentReader) {
            SegmentCommitInfo sci = segmentReader.getSegmentInfo();
            String genAttr = sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE);
            if (genAttr != null) {
                return Long.parseLong(genAttr);
            }
        }
        throw new IllegalStateException(
            "Cannot resolve writer generation for reader: "
                + reader.getClass().getName()
                + ". Ensure segments have the '"
                + WRITER_GENERATION_ATTRIBUTE
                + "' attribute."
        );
    }
}
