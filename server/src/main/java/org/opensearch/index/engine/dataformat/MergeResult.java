/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.List;
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
    private final RowIdMapping rowIdMapping;
    private final List<Segment> auxiliarySegments;

    /**
     * Constructs a merge result with the given merged writer file sets.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet) {
        this(mergedWriterFileSet, null, List.of());
    }

    /**
     * Constructs a merge result with the given merged writer file sets and row ID mapping.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     * @param rowIdMapping the row ID mapping produced during the merge
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet, RowIdMapping rowIdMapping) {
        this(mergedWriterFileSet, rowIdMapping, List.of());
    }

    /**
     * Constructs a merge result that also carries merged side tables.
     *
     * <p>Side tables are returned as whole {@link Segment}s rather than as more entries in
     * {@code mergedWriterFileSet}, because they do not belong to the merged document segment: their
     * rows are not documents, their generation is the merged generation offset by
     * {@link AuxiliaryDataFormat#GENERATION_OFFSET}, and the catalog holds them separately. This
     * mirrors {@code FileInfos#auxiliarySegments} on the refresh path.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     * @param rowIdMapping        the row ID mapping produced during the merge
     * @param auxiliarySegments   merged side table segments, each {@link Segment#isAuxiliaryOnly()}
     * @throws IllegalArgumentException if any given segment is not auxiliary-only
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet, RowIdMapping rowIdMapping, List<Segment> auxiliarySegments) {
        for (Segment segment : auxiliarySegments) {
            if (segment.isAuxiliaryOnly() == false) {
                throw new IllegalArgumentException(
                    "Merged auxiliary segment at generation ["
                        + segment.generation()
                        + "] must contain only auxiliary formats but had "
                        + segment.dfGroupedSearchableFiles().keySet()
                );
            }
        }
        this.mergedWriterFileSet = mergedWriterFileSet;
        this.rowIdMapping = rowIdMapping;
        this.auxiliarySegments = List.copyOf(auxiliarySegments);
    }

    /**
     * Returns the merged side table segments, empty if the merge had no side tables.
     *
     * @return merged auxiliary segments
     */
    public List<Segment> auxiliarySegments() {
        return auxiliarySegments;
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
     * Gets the row id mapping.
     *
     * @return the row id mapping
     */
    public Optional<RowIdMapping> rowIdMapping() {
        return Optional.ofNullable(rowIdMapping);
    }
}
