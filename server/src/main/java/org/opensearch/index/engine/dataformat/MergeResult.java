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
 * Result of a merge operation containing merged writer file sets.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeResult {

    private final Map<DataFormat, WriterFileSet> mergedWriterFileSet;
    private final Map<Long, RowIdMapping> rowIdMappings;

    /**
     * Constructs a merge result with the given merged writer file sets.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet) {
        this.mergedWriterFileSet = mergedWriterFileSet;
        this.rowIdMappings = null;
    }

    /**
     * Constructs a merge result with the given merged writer file sets and row ID mappings.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     * @param rowIdMappings the row ID mappings keyed by writer generation produced during the merge
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet, Map<Long, RowIdMapping> rowIdMappings) {
        this.mergedWriterFileSet = mergedWriterFileSet;
        this.rowIdMappings = rowIdMappings;
    }

    /**
     * Gets all merged writer file sets.
     *
     * @return map of data formats to merged writer file sets
     */
    public Map<DataFormat, WriterFileSet> getMergedWriterFileSet() {
        return mergedWriterFileSet;
    }

    /**
     * Gets the merged writer file set for a specific data format.
     *
     * @param dataFormat the data format
     * @return the merged writer file set for the specified format
     */
    public WriterFileSet getMergedWriterFileSetForDataformat(DataFormat dataFormat) {
        return mergedWriterFileSet.get(dataFormat);
    }

    /**
     * Gets the row ID mappings keyed by writer generation.
     *
     * @return optional containing the mappings, or empty if none produced
     */
    public Optional<Map<Long, RowIdMapping>> rowIdMappings() {
        return Optional.ofNullable(rowIdMappings);
    }
}
