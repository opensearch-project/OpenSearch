/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;

import com.parquet.parquetdataformat.engine.ParquetDataFormat;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowId;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.parquet.parquetdataformat.bridge.RustBridge.getTotalRows;
import static com.parquet.parquetdataformat.bridge.RustBridge.mergeParquetFilesInRust;

/**
 * Implements record-batch-based merging of Parquet files.
 */
public class RecordBatchMergeStrategy implements ParquetMergeStrategy {
    private static final AtomicLong generationCounter = new AtomicLong();

    @Override
    public MergeResult mergeParquetFiles(Collection<FileMetadata> files) {

        if (files.isEmpty()) {
            throw new IllegalArgumentException("No files to merge");
        }

        List<Path> filePaths = files.stream()
            .map(fm -> Path.of(fm.directory(), fm.file()))
            .collect(Collectors.toList());

        String outputDirectory = files.iterator().next().directory();
        long generation = generationCounter.incrementAndGet();
        String mergedFilePath = getMergedFilePath(generation, outputDirectory);
        String mergedFileName = getMergedFileName(generation);

        // Merge files in Rust
        mergeParquetFilesInRust(filePaths, mergedFilePath);

        // Build row ID mapping
        Map<RowId, Long> rowIdMapping = new HashMap<>();

        FileMetadata mergedFileMetadata = new FileMetadata(
            outputDirectory,
            mergedFileName
        );
        Map<DataFormat, Collection<FileMetadata>> mergedFiles = Collections.singletonMap(
            new ParquetDataFormat(),
            Collections.singletonList(mergedFileMetadata)
        );

        return new MergeResult(new RowIdMapping(rowIdMapping, mergedFileName), mergedFiles);
    }

    private String getMergedFileName(long generation) {
        return "generation-m-" + generation + ".parquet";
    }

    private String getMergedFilePath(long generation, String outputDirectory) {
        return Path.of(outputDirectory, getMergedFileName(generation)).toString();
    }

    private Map<RowId, Long> buildRowIdMapping(List<Path> filePaths) {
        Map<RowId, Long> rowIdMapping = new HashMap<>();
        long newRowId = 0;

        for (Path filePath : filePaths) {
            String fileIdentifier = filePath.getFileName().toString();
            String filePathStr = filePath.toString();
            long fileRowCount = getTotalRows(filePathStr);

            for (long currentRowId = 0; currentRowId < fileRowCount; currentRowId++) {
                RowId originalRowId = new RowId(currentRowId, fileIdentifier);
                rowIdMapping.put(originalRowId, newRowId++);
            }
        }

        return rowIdMapping;
    }
}
