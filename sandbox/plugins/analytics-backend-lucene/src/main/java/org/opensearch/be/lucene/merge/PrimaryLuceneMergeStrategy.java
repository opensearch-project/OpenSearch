/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Merge strategy for when Lucene is the <b>primary</b> data format in a composite index.
 *
 * <p>As the primary format, Lucene performs a merge with globally-unique row IDs
 * (offset-based via {@link PrimaryOneMerge}) and produces a {@link RowIdMapping}
 * that secondary formats use to align their document order with the merged output.
 *
 * <p>The mapping is built after the merge completes by reading the merged segment's
 * {@code __row_id__} doc values to determine how documents from each source generation
 * were reordered by IndexSort.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PrimaryLuceneMergeStrategy implements LuceneMergeStrategy {

    @Override
    public MergePolicy.OneMerge createOneMerge(List<SegmentCommitInfo> segments, RowIdMapping rowIdMapping) {
        return new PrimaryOneMerge(segments);
    }

    @Override
    public RowIdMapping buildRowIdMapping(MergePolicy.OneMerge completedMerge, MergeInput mergeInput) throws IOException {
        if (!(completedMerge instanceof PrimaryOneMerge primaryOneMerge)) {
            throw new IllegalArgumentException("Expected PrimaryOneMerge but got: " + completedMerge.getClass().getName());
        }

        SegmentCommitInfo mergedInfo = completedMerge.getMergeInfo();
        if (mergedInfo == null) {
            throw new IllegalStateException("Merged segment info is null after merge completion");
        }

        List<PrimaryOneMerge.SegmentMeta> segmentMetas = primaryOneMerge.getSegmentMetas();
        int totalDocs = primaryOneMerge.getTotalDocs();

        // Build the mapping by reading __row_id__ from the merged segment.
        // Each doc's __row_id__ value is a globally-unique offset-encoded ID set by PrimaryOneMerge.
        // Its position in the merged segment is its new row ID.
        long[] mappingArray = new long[totalDocs];
        Map<Long, Integer> generationOffsets = new HashMap<>();
        Map<Long, Integer> generationSizes = new HashMap<>();

        for (PrimaryOneMerge.SegmentMeta meta : segmentMetas) {
            generationOffsets.put(meta.generation(), meta.offset());
            generationSizes.put(meta.generation(), meta.maxDoc());
        }

        IndexWriter indexWriter = primaryOneMerge.getIndexWriter();
        if (indexWriter == null) {
            throw new IllegalStateException("IndexWriter not set on PrimaryOneMerge — cannot read merged segment");
        }

        try (DirectoryReader dirReader = DirectoryReader.open(indexWriter)) {
            LeafReader reader = findMergedSegmentReader(dirReader, mergedInfo);
            SortedNumericDocValues rowIdValues = reader.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
            if (rowIdValues == null) {
                throw new IllegalStateException("Merged segment does not contain " + DocumentInput.ROW_ID_FIELD + " doc values");
            }

            for (int newPos = 0; newPos < reader.maxDoc(); newPos++) {
                if (!rowIdValues.advanceExact(newPos)) {
                    throw new IllegalStateException("Doc " + newPos + " missing " + DocumentInput.ROW_ID_FIELD + " value");
                }
                long globalRowId = rowIdValues.nextValue();
                // globalRowId = cumulativeOffset + localRowId (set by PrimaryOneMerge)
                // mapping[globalRowId] = newPos
                mappingArray[(int) globalRowId] = newPos;
            }
        }

        return new PackedRowIdMapping(mappingArray, generationOffsets, generationSizes);
    }

    private static LeafReader findMergedSegmentReader(DirectoryReader dirReader, SegmentCommitInfo mergedInfo) {
        String mergedSegName = mergedInfo.info.name;
        for (LeafReaderContext ctx : dirReader.leaves()) {
            LeafReader leafReader = ctx.reader();
            if (leafReader instanceof SegmentReader segReader) {
                if (segReader.getSegmentInfo().info.name.equals(mergedSegName)) {
                    return leafReader;
                }
            }
        }
        throw new IllegalStateException("Could not find merged segment " + mergedSegName + " in directory reader");
    }
}
