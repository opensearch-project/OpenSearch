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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Container for file information organized by data format.
 *
 * @param writerFilesMap     the flushed files of the writer's own table, one entry per data format
 * @param rowIdMapping       the permutation applied if the primary sorted on close, else null
 * @param auxiliarySegments  complete side-table segments the writer produced alongside its own
 *                           table; see {@link #auxiliarySegments()}
 * @opensearch.experimental
 */
@ExperimentalApi
public record FileInfos(Map<DataFormat, WriterFileSet> writerFilesMap, RowIdMapping rowIdMapping, List<Segment> auxiliarySegments) {

    public FileInfos {
        writerFilesMap = Map.copyOf(new HashMap<>(writerFilesMap));
        auxiliarySegments = List.copyOf(auxiliarySegments);
    }

    /**
     * Constructs FileInfos with no auxiliary segments.
     */
    public FileInfos(Map<DataFormat, WriterFileSet> writerFilesMap, RowIdMapping rowIdMapping) {
        this(writerFilesMap, rowIdMapping, List.of());
    }

    /**
     * Constructs FileInfos without a sort permutation.
     */
    public FileInfos(Map<DataFormat, WriterFileSet> writerFilesMap) {
        this(writerFilesMap, (RowIdMapping) null);
    }

    /**
     * Gets the writer file set for a specific data format.
     *
     * @param format the data format
     * @return an Optional containing the writer file set, or empty if not found
     */
    public Optional<WriterFileSet> getWriterFileSet(DataFormat format) {
        return Optional.ofNullable(writerFilesMap.get(format));
    }

    /**
     * Returns side-table segments this writer produced alongside its own table — currently the
     * nested child table, which holds one row per nested element.
     *
     * <p>These are whole {@link Segment}s rather than extra {@code writerFilesMap} entries because a
     * side table has its <em>own</em> generation and its own row count. Both matter: a segment's
     * generation is what a caller uses to name it, and {@code CatalogSnapshotManager} asserts that
     * every format inside one segment reports the same row count — which a parent and its child
     * cannot both satisfy. Their formats must be
     * {@linkplain DataFormat#isAuxiliaryFormatName auxiliary} so that document counts derived from
     * the segment list exclude them.
     *
     * @return the auxiliary segments, empty if the writer produced none
     */
    public List<Segment> auxiliarySegments() {
        return auxiliarySegments;
    }

    /**
     * Creates an empty FileInfos instance.
     *
     * @return an empty FileInfos
     */
    public static FileInfos empty() {
        return new FileInfos(Map.of());
    }

    /**
     * Creates a new builder for FileInfos.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for constructing FileInfos instances.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static final class Builder {
        private final Map<DataFormat, WriterFileSet> writerFilesMap = new HashMap<>();
        private final List<Segment> auxiliarySegments = new ArrayList<>();
        private RowIdMapping rowIdMapping;

        /**
         * Adds a writer file set for a specific data format.
         *
         * @param format the data format
         * @param writerFileSet the writer file set
         * @return this builder
         */
        public Builder putWriterFileSet(DataFormat format, WriterFileSet writerFileSet) {
            writerFilesMap.put(format, writerFileSet);
            return this;
        }

        /**
         * Adds all entries from the provided map.
         *
         * @param map the map of data formats to writer file sets
         * @return this builder
         */
        public Builder putAll(Map<DataFormat, WriterFileSet> map) {
            writerFilesMap.putAll(map);
            return this;
        }

        /**
         * Sets the row ID mapping produced during sort-on-close.
         *
         * @param rowIdMapping the row ID mapping, or null
         * @return this builder
         */
        public Builder rowIdMapping(RowIdMapping rowIdMapping) {
            this.rowIdMapping = rowIdMapping;
            return this;
        }

        /**
         * Adds a side-table segment produced alongside this writer's own table. Every format in
         * {@code segment} must be {@linkplain DataFormat#isAuxiliaryFormatName auxiliary}, or
         * document counts derived from the segment list would include its rows.
         *
         * @param segment the auxiliary segment
         * @return this builder
         * @throws IllegalArgumentException if the segment is not auxiliary-only
         */
        public Builder addAuxiliarySegment(Segment segment) {
            if (segment.isAuxiliaryOnly() == false) {
                throw new IllegalArgumentException(
                    "Segment at generation ["
                        + segment.generation()
                        + "] carries formats "
                        + segment.dfGroupedSearchableFiles().keySet()
                        + ", which are not all prefixed ["
                        + DataFormat.AUXILIARY_NAME_PREFIX
                        + "]; registering it as auxiliary would let its rows be counted as documents"
                );
            }
            auxiliarySegments.add(segment);
            return this;
        }

        /**
         * Builds the FileInfos instance.
         *
         * @return a new FileInfos instance
         */
        public FileInfos build() {
            return new FileInfos(writerFilesMap, rowIdMapping, auxiliarySegments);
        }
    }
}
