/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetIndexingEngine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Implements merging of Parquet files.
 */
public class NativeParquetMergeStrategy implements ParquetMergeStrategy {

    private static final Logger logger = LogManager.getLogger(NativeParquetMergeStrategy.class);

    private final DataFormat dataFormat;
    private final String indexName;
    private final Path shardDataPath;

    public NativeParquetMergeStrategy(DataFormat dataFormat, String indexName, Path shardDataPath) {
        this.dataFormat = dataFormat;
        this.indexName = indexName;
        this.shardDataPath = shardDataPath;
    }

    @Override
    public MergeResult mergeParquetFiles(MergeInput mergeInput) {

        List<WriterFileSet> files = mergeInput.getFilesForFormat(dataFormat.name());
        long writerGeneration = mergeInput.newWriterGeneration();
        if (files.isEmpty()) {
            throw new IllegalArgumentException("No files to merge");
        }

        List<Path> filePaths = new ArrayList<>();
        files.forEach(
            writerFileSet -> writerFileSet.files()
                .forEach(file -> filePaths.add(shardDataPath.resolve(writerFileSet.directory()).resolve(file)))
        );

        String outputDirectory = shardDataPath.resolve(files.getFirst().directory()).toString();
        String mergedFilePath = getMergedFilePath(writerGeneration, outputDirectory);
        String mergedFileName = getMergedFileName(writerGeneration);

        try {
            // Merge files in Rust
            RustBridge.mergeParquetFilesInRust(filePaths, mergedFilePath, indexName);

            WriterFileSet mergedWriterFileSet = WriterFileSet.builder()
                .directory(Path.of(files.getFirst().directory()))
                .addFile(mergedFileName)
                .writerGeneration(writerGeneration)
                .build();

            Map<DataFormat, WriterFileSet> mergedWriterFileSetMap = Collections.singletonMap(dataFormat, mergedWriterFileSet);

            return new MergeResult(mergedWriterFileSetMap);

        } catch (Exception exception) {
            logger.error(() -> new ParameterizedMessage("Merge failed while creating merged file [{}]", mergedFilePath), exception);
            try {
                Files.deleteIfExists(Path.of(mergedFilePath));
                logger.info("Stale Merged File Deleted at : [{}]", mergedFilePath);
            } catch (Exception innerException) {
                logger.error(() -> new ParameterizedMessage("Failed to delete stale merged file [{}]", mergedFilePath), innerException);

            }
            throw exception;
        }

    }

    private String getMergedFileName(long generation) {
        // TODO: For debugging we have added extra "merged" in file name, later we can remove and keep same as writer
        return ParquetIndexingEngine.FILE_NAME_PREFIX + "_merged_" + generation + ParquetIndexingEngine.FILE_NAME_EXT;
    }

    private String getMergedFilePath(long generation, String outputDirectory) {
        return Path.of(outputDirectory, getMergedFileName(generation)).toString();
    }
}
