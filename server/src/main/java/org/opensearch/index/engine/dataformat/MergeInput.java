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
import java.util.List;
import java.util.Objects;

/**
 * Input data for a merge operation.
 * Use {@link Builder} to construct instances.
 *
 * <p>{@code liveDocs} provides per-segment delete filtering. See {@link LiveDocs} for
 * the contract. The snapshot is taken once at merge start; mid-merge deletes are not
 * reflected.</p>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record MergeInput(List<Segment> segments, RowIdMapping rowIdMapping, long newWriterGeneration, LiveDocs liveDocs) {

    public MergeInput {
        segments = List.copyOf(segments);
        Objects.requireNonNull(liveDocs, "liveDocs must not be null; use LiveDocs.ALL_ALIVE for no filtering");
    }

    private MergeInput(Builder builder) {
        this(new ArrayList<>(builder.segments), builder.rowIdMapping, builder.newWriterGeneration, builder.liveDocs);
    }

    /**
     * Returns the {@link WriterFileSet} for the given data format from each segment.
     *
     * @param formatName the data format name (e.g. "parquet")
     * @return list of writer file sets for the format across all segments
     */
    public List<WriterFileSet> getFilesForFormat(String formatName) {
        return segments.stream().map(seg -> seg.dfGroupedSearchableFiles().get(formatName)).filter(Objects::nonNull).toList();
    }

    /**
     * Returns the live-docs bitset for the given segment generation, or {@code null} if all
     * rows in that segment are alive. Convenience delegate to {@link LiveDocs#packedBits(long)}.
     */
    public long[] getLiveDocsForSegment(long generation) {
        return liveDocs.packedBits(generation);
    }

    /**
     * Returns a new builder for constructing {@link MergeInput} instances.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link MergeInput}.
     */
    @ExperimentalApi
    public static class Builder {
        private List<Segment> segments = new ArrayList<>();
        private RowIdMapping rowIdMapping;
        private long newWriterGeneration;
        private LiveDocs liveDocs = LiveDocs.ALL_ALIVE;

        private Builder() {}

        /**
         * Sets the list of segments to merge.
         *
         * @param segments the segments to merge
         * @return this builder
         */
        public Builder segments(List<Segment> segments) {
            this.segments = new ArrayList<>(segments);
            return this;
        }

        /**
         * Adds a segment to merge.
         *
         * @param segment the segment to add
         * @return this builder
         */
        public Builder addSegment(Segment segment) {
            this.segments.add(segment);
            return this;
        }

        /**
         * Sets the row ID mapping for secondary data format merges.
         *
         * @param rowIdMapping the row ID mapping
         * @return this builder
         */
        public Builder rowIdMapping(RowIdMapping rowIdMapping) {
            this.rowIdMapping = rowIdMapping;
            return this;
        }

        /**
         * Sets the writer generation for the merged output.
         *
         * @param newWriterGeneration the new writer generation
         * @return this builder
         */
        public Builder newWriterGeneration(long newWriterGeneration) {
            this.newWriterGeneration = newWriterGeneration;
            return this;
        }

        /**
         * Sets the live-docs for delete filtering during merge.
         *
         * @param liveDocs the live-docs instance
         * @return this builder
         */
        public Builder liveDocs(LiveDocs liveDocs) {
            this.liveDocs = Objects.requireNonNull(liveDocs, "liveDocs must not be null; use LiveDocs.ALL_ALIVE");
            return this;
        }

        /**
         * Builds an immutable {@link MergeInput}.
         *
         * @return the constructed MergeInput
         */
        public MergeInput build() {
            return new MergeInput(this);
        }
    }
}
