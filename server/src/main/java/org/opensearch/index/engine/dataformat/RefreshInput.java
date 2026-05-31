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

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable input data for a refresh operation, containing existing segments and writer files.
 * Use {@link Builder} to construct instances.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record RefreshInput(List<Segment> existingSegments, List<Segment> writerFiles, long nextAvailableGeneration) {

    /** Sentinel indicating no spare generation was allocated. */
    public static final long NO_GENERATION = -1L;

    public RefreshInput(List<Segment> existingSegments, List<Segment> writerFiles) {
        this(existingSegments, writerFiles, NO_GENERATION);
    }

    public RefreshInput {
        existingSegments = List.copyOf(existingSegments);
        writerFiles = List.copyOf(writerFiles);
    }

    /**
     * Whether a spare generation is available for engines that want to merge
     * multiple writer files into a single segment during refresh.
     * The engine decides whether to actually use it.
     */
    public boolean hasNextGeneration() {
        return nextAvailableGeneration > 0 && writerFiles.size() > 1;
    }

    /**
     * Returns a new builder for constructing {@link RefreshInput} instances.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link RefreshInput}.
     */
    @ExperimentalApi
    public static class Builder {
        private List<Segment> existingSegments = new ArrayList<>();
        private List<Segment> segments = new ArrayList<>();
        private long nextAvailableGeneration = NO_GENERATION;

        private Builder() {}

        /**
         * Sets the existing segments.
         *
         * @param existingSegments the list of existing segments
         * @return this builder
         */
        public Builder existingSegments(List<Segment> existingSegments) {
            this.existingSegments = new ArrayList<>(existingSegments);
            return this;
        }

        /**
         * Adds a writer file set.
         *
         * @param segment the segment set to add
         * @return this builder
         */
        public Builder addSegment(Segment segment) {
            this.segments.add(segment);
            return this;
        }

        /**
         * Sets the next available generation that the engine may use if it decides
         * to merge writer files during refresh.
         */
        public Builder nextAvailableGeneration(long generation) {
            this.nextAvailableGeneration = generation;
            return this;
        }

        public RefreshInput build() {
            return new RefreshInput(existingSegments, segments, nextAvailableGeneration);
        }
    }
}
