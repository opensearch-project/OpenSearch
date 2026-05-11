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
import java.util.Objects;

/**
 * input data for a merge operation.
 * Use {@link Builder} to construct instances.
 *
 * <p>{@code liveDocsPerSegment} is keyed by {@link Segment#generation()}. Values are packed
 * bitsets in Lucene {@code FixedBitSet#getBits()} layout (bit {@code i} set means row
 * {@code i} is alive). Absent key = all rows alive. The snapshot is taken once at merge
 * start; mid-merge deletes are not reflected.</p>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record MergeInput(
    List<Segment> segments,
    RowIdMapping rowIdMapping,
    long newWriterGeneration,
    Map<Long, long[]> liveDocsPerSegment
) {

    public MergeInput {
        segments = List.copyOf(segments);
        liveDocsPerSegment = Map.copyOf(liveDocsPerSegment);
    }

    private MergeInput(Builder builder) {
        this(
            new ArrayList<>(builder.segments),
            builder.rowIdMapping,
            builder.newWriterGeneration,
            new HashMap<>(builder.liveDocsPerSegment)
        );
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
     * rows in that segment are alive.
     */
    public long[] getLiveDocsForSegment(long generation) {
        return liveDocsPerSegment.get(generation);
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
        private Map<Long, long[]> liveDocsPerSegment = Map.of();

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
         * Sets the per-segment live-docs bitsets. See class javadoc for the contract.
         */
        public Builder liveDocsPerSegment(Map<Long, long[]> liveDocsPerSegment) {
            this.liveDocsPerSegment = Objects.requireNonNull(liveDocsPerSegment, "liveDocsPerSegment must not be null");
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
