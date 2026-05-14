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
 * <p>This class also stamps the {@link org.opensearch.be.lucene.index.LuceneWriter#WRITER_GENERATION_ATTRIBUTE}
 * onto the merged segment's {@link org.apache.lucene.index.SegmentInfo} via
 * {@link #setMergeInfo(SegmentCommitInfo)}. Lucene's {@code IndexWriter.mergeMiddle} invokes
 * this hook immediately before calling {@code codec.segmentInfoFormat().write(...)} on the
 * merged segment, so the attribute is persisted to the {@code .si} file and survives a
 * writer reopen. No codec, thread-local, or commit-data plumbing is needed.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class RowIdRemappingOneMerge extends MergePolicy.OneMerge {

    private final RowIdMapping rowIdMapping;
    private final long outputWriterGeneration;
    private int nextRowIdOffset;

    RowIdRemappingOneMerge(List<SegmentCommitInfo> segments, RowIdMapping rowIdMapping, long outputWriterGeneration) {
        super(segments);
        this.rowIdMapping = rowIdMapping;
        this.outputWriterGeneration = outputWriterGeneration;
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

    /**
     * Stamps the writer generation attribute on the merged segment so it is persisted when
     * Lucene writes the {@code .si} file.
     *
     * <p>Lucene's {@code IndexWriter} calls this hook twice during a merge: once in
     * {@code _mergeInit} right after the output {@link SegmentCommitInfo} is created, and a
     * second time in {@code mergeMiddle} immediately before
     * {@code codec.segmentInfoFormat().write(...)} persists the {@code .si}. The second call
     * is the one that causes the attribute to land on disk. Stamping is idempotent, so the
     * double-invocation is harmless.
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
