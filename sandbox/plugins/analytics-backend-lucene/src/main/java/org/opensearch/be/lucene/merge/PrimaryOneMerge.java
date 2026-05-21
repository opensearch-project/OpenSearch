/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * A custom {@link MergePolicy.OneMerge} for when Lucene is the <b>primary</b> data format.
 *
 * <p>Rewrites {@code __row_id__} doc values to be globally unique across all input segments
 * by offsetting each segment's local row IDs by a cumulative doc count. After merge, the
 * globally-unique values in the merged segment reveal the sort permutation, which is used
 * to build a {@link RowIdMapping} for secondary formats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PrimaryOneMerge extends MergePolicy.OneMerge {

    private final List<SegmentMeta> segmentMetas = new ArrayList<>();
    private int nextOffset;
    private IndexWriter indexWriter;

    public PrimaryOneMerge(List<SegmentCommitInfo> segments) {
        super(segments);
        this.nextOffset = 0;
    }

    /**
     * Sets the IndexWriter used for opening an NRT reader after merge.
     */
    public void setIndexWriter(IndexWriter indexWriter) {
        this.indexWriter = indexWriter;
    }

    /**
     * Returns the IndexWriter for post-merge reading.
     */
    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
        CodecReader wrapped = super.wrapForMerge(reader);
        long generation = resolveGeneration(wrapped);
        int offset = nextOffset;
        int maxDoc = wrapped.maxDoc();
        segmentMetas.add(new SegmentMeta(generation, maxDoc, offset));
        nextOffset += maxDoc;

        RowIdMapping offsetMapping = buildOffsetMapping();
        return new RowIdRemappingCodecReader(wrapped, offsetMapping, generation, offset);
    }

    /**
     * Builds a {@link PackedRowIdMapping} that maps local row IDs to globally unique IDs
     * using cumulative offsets. Called during wrapForMerge after all segment metas are recorded.
     */
    private RowIdMapping buildOffsetMapping() {
        int totalDocs = nextOffset;
        long[] mappingArray = new long[totalDocs];
        Map<Long, Integer> generationOffsets = new HashMap<>();
        Map<Long, Integer> generationSizes = new HashMap<>();

        for (SegmentMeta meta : segmentMetas) {
            generationOffsets.put(meta.generation, meta.offset);
            generationSizes.put(meta.generation, meta.maxDoc);
            for (int i = 0; i < meta.maxDoc; i++) {
                mappingArray[meta.offset + i] = meta.offset + i;
            }
        }

        return new PackedRowIdMapping(mappingArray, generationOffsets, generationSizes);
    }

    /**
     * Returns the segment metadata recorded during wrapForMerge.
     */
    public List<SegmentMeta> getSegmentMetas() {
        return List.copyOf(segmentMetas);
    }

    /**
     * Returns the total number of documents across all input segments.
     */
    public int getTotalDocs() {
        return nextOffset;
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

    /**
     * Metadata for one input segment in the merge.
     */
    public record SegmentMeta(long generation, int maxDoc, int offset) {
    }
}
