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
public record RefreshInput(List<Segment> existingSegments, List<Segment> writerFiles) {

    public RefreshInput {
        existingSegments = List.copyOf(existingSegments);
        writerFiles = List.copyOf(writerFiles);
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
         * Builds an immutable {@link RefreshInput}.
         *
         * @return the constructed RefreshInput
         */
        public RefreshInput build() {
            return new RefreshInput(existingSegments, segments);
        }
    }
}
