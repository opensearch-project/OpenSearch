/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Result of merging a single data format's files.
 */
@ExperimentalApi
public record FormatMergeResult(DataFormat format, WriterFileSet mergedFiles, RowIdMapping rowIdMapping) {

    public Optional<RowIdMapping> rowIdMappingOpt() {
        return Optional.ofNullable(rowIdMapping);
    }

    /**
     * Deletes the merged output files. Called during cleanup on merge failure.
     */
    public void cleanup() {
        if (mergedFiles == null) return;
        for (String file : mergedFiles.files()) {
            try {
                Path resolved = mergedFiles.directory() != null ? Path.of(mergedFiles.directory(), file) : Path.of(file);
                Files.deleteIfExists(resolved);
            } catch (IOException ignored) {
                // Best-effort cleanup
            }
        }
    }
}
