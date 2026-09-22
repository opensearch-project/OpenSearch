/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A mock {@link Merger} for testing purposes.
 */
public class MockMerger implements Merger {
    private final DataFormat dataFormat;
    private final Path directory;

    public MockMerger(DataFormat dataFormat, Path directory) {
        this.dataFormat = dataFormat;
        this.directory = directory;
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) {
        List<WriterFileSet> fileMetadataList = new ArrayList<>();
        for (Segment segment : mergeInput.segments()) {
            fileMetadataList.addAll(segment.dfGroupedSearchableFiles().values());
        }
        long newWriterGeneration = mergeInput.newWriterGeneration();
        RowIdMapping existingMapping = mergeInput.rowIdMapping();

        String prefix = existingMapping != null ? "secondary_merged_gen" : "merged_gen";
        WriterFileSet merged = WriterFileSet.builder()
            .directory(directory)
            .writerGeneration(newWriterGeneration)
            .addFile(prefix + newWriterGeneration + ".parquet")
            .addNumRows(fileMetadataList.stream().mapToLong(WriterFileSet::numRows).sum())
            .build();

        if (existingMapping != null) {
            return new MergeResult(Map.of(dataFormat, merged), existingMapping);
        }

        Map<Long, Long> genOffsets = new HashMap<>();
        Map<Long, Integer> genOffsetsInt = new HashMap<>();
        Map<Long, Integer> genSizesInt = new HashMap<>();
        int offset = 0;
        for (WriterFileSet fs : fileMetadataList) {
            genOffsets.put(fs.writerGeneration(), (long) offset);
            genOffsetsInt.put(fs.writerGeneration(), offset);
            genSizesInt.put(fs.writerGeneration(), (int) fs.numRows());
            offset += (int) fs.numRows();
        }
        long[] mappingArray = new long[offset];
        for (int i = 0; i < offset; i++) {
            mappingArray[i] = i;
        }
        RowIdMapping mapping = new PackedRowIdMapping(mappingArray, genOffsetsInt, genSizesInt);

        return new MergeResult(Map.of(dataFormat, merged), mapping);
    }
}
