/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;

import com.parquet.parquetdataformat.engine.ParquetDataFormat;
import com.parquet.parquetdataformat.engine.ParquetExecutionEngine;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowId;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.parquet.parquetdataformat.bridge.RustBridge.mergeParquetFilesInRust;

/**
 * Implements record-batch-based merging of Parquet files.
 */
public class RecordBatchMergeStrategy implements ParquetMergeStrategy {

    @Override
    public MergeResult mergeParquetFiles(List<WriterFileSet> files, long writerGeneration) {

        if (files.isEmpty()) {
            throw new IllegalArgumentException("No files to merge");
        }

        List<Path> filePaths = new ArrayList<>();
        files.forEach(writerFileSet ->  writerFileSet.getFiles().forEach(
            file -> filePaths.add(Path.of(writerFileSet.getDirectory(), file))));

        FileMetadata firstFile = files.iterator().next();
        String outputDirectory = firstFile.directory();
        String dataFormat = firstFile.dataFormat();
        long generation = generationCounter.incrementAndGet();
        String mergedFilePath = getMergedFilePath(generation, outputDirectory);
        String mergedFileName = getMergedFileName(generation);

        // Merge files in Rust
        mergeParquetFilesInRust(filePaths, mergedFilePath);

        // Build row ID mapping
        Map<RowId, Long> rowIdMapping = new HashMap<>();

        FileMetadata mergedFileMetadata = new FileMetadata(
            dataFormat,
            outputDirectory,
            mergedFileName
        );
        Map<DataFormat, Collection<FileMetadata>> mergedFiles = Collections.singletonMap(
            new ParquetDataFormat(),
            mergedWriterFileSet
        );

        return new MergeResult(new RowIdMapping(rowIdMapping, mergedFileName), mergedWriterFileSetMap);
    }

    private String getMergedFileName(long generation) {
        // TODO
        // For debuging we have added extra "merged" in file name, later we can remove and keep same as writer
        return ParquetExecutionEngine.FILE_NAME_PREFIX + "_merged_" + generation + ParquetExecutionEngine.FILE_NAME_EXT;
    }

    private String getMergedFilePath(long generation, String outputDirectory) {
        return Path.of(outputDirectory, getMergedFileName(generation)).toString();
    }
}
