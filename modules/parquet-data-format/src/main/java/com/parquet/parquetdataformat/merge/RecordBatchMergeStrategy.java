/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowId;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.parquet.parquetdataformat.bridge.RustBridge.mergeParquetFilesInRust;

/**
 * Implements record-batch-based merging of Parquet files.
 */
public class RecordBatchMergeStrategy implements ParquetMergeStrategy {

    @Override
    public MergeResult mergeParquetFiles(Collection<FileMetadata> files) {
        Set<Path> filePaths = files.stream()
            .map(fm -> new File(fm.directory(), fm.file()).toPath())
            .collect(Collectors.toSet());

        //todo: fetch correct outputPath
        String outputPath = "~/";

        mergeParquetFilesInRust(filePaths, outputPath);
        Map<RowId, RowId> mapping = new HashMap<>();
        RowIdMapping rowIdMapping = new RowIdMapping(mapping);
        Map<DataFormat, Collection<FileMetadata>> merged = new HashMap<>();

        //todo: populate MergeResult with actual rowIdMapping and mergedFileMetadata
        return new MergeResult(rowIdMapping, merged);
    }
}
