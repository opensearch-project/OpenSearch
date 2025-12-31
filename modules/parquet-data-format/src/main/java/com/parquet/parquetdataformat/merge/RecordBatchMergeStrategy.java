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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowId;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.parquet.parquetdataformat.bridge.RustBridge.mergeParquetFilesInRust;

/**
 * Implements record-batch-based merging of Parquet files.
 */
public class RecordBatchMergeStrategy implements ParquetMergeStrategy {

    private static final Logger logger =
        LogManager.getLogger(RecordBatchMergeStrategy.class);

    @Override
    public MergeResult mergeParquetFiles(List<WriterFileSet> files, long writerGeneration) {

        if (files.isEmpty()) {
            throw new IllegalArgumentException("No files to merge");
        }

        List<Path> filePaths = new ArrayList<>();
        files.forEach(writerFileSet ->  writerFileSet.getFiles().forEach(
            file -> filePaths.add(Path.of(writerFileSet.getDirectory(), file))));

        String outputDirectory = files.iterator().next().getDirectory();
        String mergedFilePath = getMergedFilePath(writerGeneration, outputDirectory);
        String mergedFileName = getMergedFileName(writerGeneration);

        try {
            // Merge files in Rust
            mergeParquetFilesInRust(filePaths, mergedFilePath);

            // Build row ID mapping
            Map<RowId, Long> rowIdMapping = new HashMap<>();

            WriterFileSet mergedWriterFileSet =
            WriterFileSet.builder().directory(Path.of(outputDirectory)).addFile(mergedFileName).writerGeneration(writerGeneration).build();

            Map<DataFormat, WriterFileSet> mergedWriterFileSetMap = Collections.singletonMap(
                new ParquetDataFormat(),
                mergedWriterFileSet
            );

            return new MergeResult(new RowIdMapping(rowIdMapping, mergedFileName), mergedWriterFileSetMap);

        } catch (Exception exception) {
            logger.error(
                () -> new ParameterizedMessage(
                    "Merge failed while creating merged file [{}]",
                    mergedFilePath
                ),
                exception
            );
            try {
                Files.deleteIfExists(Path.of(mergedFilePath));
                logger.info("Stale Merged File Deleted at : [{}]", mergedFilePath);
            } catch (Exception innerException) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "Failed to delete stale merged file [{}]",
                        mergedFilePath
                    ),
                    innerException
                );

            }
            throw exception;
        }

    }

    private String getMergedFileName(long generation) {
        // TODO: For debugging we have added extra "merged" in file name, later we can remove and keep same as writer
        return ParquetExecutionEngine.FILE_NAME_PREFIX + "_merged_" + generation + ParquetExecutionEngine.FILE_NAME_EXT;
    }

    private String getMergedFilePath(long generation, String outputDirectory) {
        return Path.of(outputDirectory, getMergedFileName(generation)).toString();
    }
}
