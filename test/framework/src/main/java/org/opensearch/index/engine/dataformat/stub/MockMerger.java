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
        Map<Long, RowIdMapping> existingMappings = mergeInput.rowIdMappings();

        String prefix = (existingMappings != null && !existingMappings.isEmpty()) ? "secondary_merged_gen" : "merged_gen";
        WriterFileSet merged = WriterFileSet.builder()
            .directory(directory)
            .writerGeneration(newWriterGeneration)
            .addFile(prefix + newWriterGeneration + ".parquet")
            .addNumRows(fileMetadataList.stream().mapToLong(WriterFileSet::numRows).sum())
            .build();

        if (existingMappings != null && !existingMappings.isEmpty()) {
            return new MergeResult(Map.of(dataFormat, merged), existingMappings);
        }

        // Build one PackedRowIdMapping per generation with global row ID assignment
        Map<Long, RowIdMapping> mappings = new HashMap<>();
        int globalOffset = 0;
        for (WriterFileSet fs : fileMetadataList) {
            int size = (int) fs.numRows();
            long[] mappingArray = new long[size];
            for (int i = 0; i < size; i++) {
                mappingArray[i] = globalOffset + i;
            }
            mappings.put(fs.writerGeneration(), new PackedRowIdMapping(mappingArray, false));
            globalOffset += size;
        }

        return new MergeResult(Map.of(dataFormat, merged), mappings);
    }
}
