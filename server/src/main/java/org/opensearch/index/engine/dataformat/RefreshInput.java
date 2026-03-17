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
import java.util.Collections;
import java.util.List;

/**
 * Immutable input data for a refresh operation, containing existing segments and writer files.
 * Use {@link Builder} to construct instances.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RefreshInput {

    private final List<Segment> existingSegments;
    private final List<WriterFileSet> writerFiles;

    private RefreshInput(Builder builder) {
        this.existingSegments = Collections.unmodifiableList(new ArrayList<>(builder.existingSegments));
        this.writerFiles = Collections.unmodifiableList(new ArrayList<>(builder.writerFiles));
    }

    /**
     * Gets the list of writer files.
     *
     * @return an unmodifiable list of writer files
     */
    public List<WriterFileSet> getWriterFiles() {
        return writerFiles;
    }

    /**
     * Gets the list of existing segments.
     *
     * @return an unmodifiable list of existing segments
     */
    public List<Segment> getExistingSegments() {
        return existingSegments;
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
        private List<WriterFileSet> writerFiles = new ArrayList<>();

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
         * @param writerFileSet the writer file set to add
         * @return this builder
         */
        public Builder addWriterFileSet(WriterFileSet writerFileSet) {
            this.writerFiles.add(writerFileSet);
            return this;
        }

        /**
         * Builds an immutable {@link RefreshInput}.
         *
         * @return the constructed RefreshInput
         */
        public RefreshInput build() {
            return new RefreshInput(this);
        }
    }
}
