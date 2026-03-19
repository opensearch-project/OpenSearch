/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.Map;
import java.util.Optional;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public final class MergeResult {

    private final Map<DataFormat, WriterFileSet> mergedFiles;
    private final RowIdMapping rowIdMapping;

    public MergeResult(Map<DataFormat, WriterFileSet> mergedFiles) {
        this(mergedFiles, null);
    }

    public MergeResult(Map<DataFormat, WriterFileSet> mergedFiles, RowIdMapping rowIdMapping) {
        this.mergedFiles = mergedFiles;
        this.rowIdMapping = rowIdMapping;
    }

    public WriterFileSet getMergedWriterFileSetForDataformat(DataFormat format) {
        return mergedFiles.get(format);
    }

    public Optional<RowIdMapping> rowIdMapping() {
        return Optional.ofNullable(rowIdMapping);
    }
}
