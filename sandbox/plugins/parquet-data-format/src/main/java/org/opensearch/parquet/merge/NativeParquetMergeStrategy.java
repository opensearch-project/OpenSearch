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
import org.opensearch.common.TriConsumer;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.merge.MergePreflightChecker;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.parquet.bridge.MergeFilesResult;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
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
    private final IndexSettings indexSettings;
    private final ShardPath shardPath;
    private TriConsumer<String, Long, Long> checksumUpdater;

    public NativeParquetMergeStrategy(
        DataFormat dataFormat,
        IndexSettings indexSettings,
        ShardPath shardPath,
        TriConsumer<String, Long, Long> checksumUpdater
    ) {
        this.dataFormat = dataFormat;
        this.indexSettings = indexSettings;
        this.shardPath = shardPath;
        this.checksumUpdater = checksumUpdater;
    }

    @Override
    public MergeResult mergeParquetFiles(MergeInput mergeInput) {

        List<WriterFileSet> files = mergeInput.getFilesForFormat(dataFormat.name());
        long writerGeneration = mergeInput.newWriterGeneration();
        if (files.isEmpty()) {
            throw new IllegalArgumentException("No files to merge");
        }
        assert writerGeneration > 0 : "merge writer generation must be positive but was: " + writerGeneration;

        List<Path> filePaths = new ArrayList<>();
        files.forEach(
            writerFileSet -> writerFileSet.files().forEach(file -> filePaths.add(Path.of(writerFileSet.directory()).resolve(file)))
        );
        assert filePaths.isEmpty() == false : "must have at least one input file path for merge";
        // All input files must exist on disk before invoking the native merge
        // This will change to object store lookup once warm is in place
        assert filePaths.stream().allMatch(p -> java.nio.file.Files.exists(p)) : "all input files must exist on disk before merge: "
            + filePaths.stream().filter(p -> java.nio.file.Files.exists(p) == false).toList();

        Path mergedFilePath = ParquetIndexingEngine.buildParquetFilePath(shardPath, writerGeneration, "merged");
        String mergedFileName = mergedFilePath.getFileName().toString();

        // Pre-merge disk space guard: reject the merge if the target file system does not
        // have enough usable space to hold the projected merged output. Throws
        // InsufficientDiskSpaceException — propagates as a hard merge failure.
        long estimatedInputBytes = files.stream().mapToLong(WriterFileSet::getTotalSize).sum();
        try {
            MergePreflightChecker.check(indexSettings, mergedFilePath.getParent(), estimatedInputBytes, dataFormat.name());
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("Failed to evaluate pre-merge disk space for [" + mergedFilePath + "]", e);
        }

        try {
            // Merge files in Rust
            MergeFilesResult merged = RustBridge.mergeParquetFilesInRust(filePaths, mergedFilePath.toString(), indexSettings.getIndex().getName());
            ParquetFileMetadata mergeMetadata = merged.metadata();
            RowIdMapping rowIdMapping = merged.rowIdMapping();

            assert mergeMetadata.numRows() > 0 : "Merged file should contain at least one row";

            long expectedRows = files.stream().mapToLong(WriterFileSet::numRows).sum();
            assert mergeMetadata.numRows() == expectedRows : "Merged row count ["
                + mergeMetadata.numRows()
                + "] must equal sum of input row counts ["
                + expectedRows
                + "]";

            WriterFileSet mergedWriterFileSet = WriterFileSet.builder()
                .directory(mergedFilePath.getParent().toAbsolutePath())
                .addFile(mergedFileName)
                .writerGeneration(writerGeneration)
                .addNumRows(mergeMetadata.numRows())
                .build();

            checksumUpdater.apply(mergedFileName, mergeMetadata.crc32(), mergeInput.newWriterGeneration());
            Map<DataFormat, WriterFileSet> mergedWriterFileSetMap = Collections.singletonMap(dataFormat, mergedWriterFileSet);

            return new MergeResult(mergedWriterFileSetMap, rowIdMapping);

        } catch (Exception exception) {
            logger.error(() -> new ParameterizedMessage("Merge failed while creating merged file [{}]", mergedFilePath), exception);
            try {
                Files.deleteIfExists(mergedFilePath);
                logger.info("Stale Merged File Deleted at : [{}]", mergedFilePath);
            } catch (Exception innerException) {
                logger.error(() -> new ParameterizedMessage("Failed to delete stale merged file [{}]", mergedFilePath), innerException);

            }
            throw exception;
        }

    }

    private String getMergedFileName(long generation) {
        // TODO: For debugging we have added extra "merged" in file name, later we can remove and keep same as writer
        return ParquetIndexingEngine.buildParquetFileName(generation, "merged");
    }
}
