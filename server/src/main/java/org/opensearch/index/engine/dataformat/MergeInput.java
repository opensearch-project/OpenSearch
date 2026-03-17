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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * input data for a merge operation.
 * Use {@link Builder} to construct instances.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeInput {

    private final List<WriterFileSet> fileMetadataList;
    private final RowIdMapping rowIdMapping;
    private final long newWriterGeneration;

    private MergeInput(Builder builder) {
        this.fileMetadataList = Collections.unmodifiableList(new ArrayList<>(builder.fileMetadataList));
        this.rowIdMapping = builder.rowIdMapping;
        this.newWriterGeneration = builder.newWriterGeneration;
    }

    /**
     * Gets the list of writer file sets to merge.
     *
     * @return an unmodifiable list of writer file sets
     */
    public List<WriterFileSet> getFileMetadataList() {
        return fileMetadataList;
    }

    /**
     * Gets the optional row ID mapping for secondary data format merges.
     *
     * @return the row ID mapping, or null if not provided
     */
    public RowIdMapping getRowIdMapping() {
        return rowIdMapping;
    }

    /**
     * Gets the writer generation for the merged output.
     *
     * @return the new writer generation
     */
    public long getNewWriterGeneration() {
        return newWriterGeneration;
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
        private List<WriterFileSet> fileMetadataList = new ArrayList<>();
        private RowIdMapping rowIdMapping;
        private long newWriterGeneration;

        private Builder() {}

        /**
         * Sets the list of writer file sets to merge.
         *
         * @param fileMetadataList the writer file sets
         * @return this builder
         */
        public Builder fileMetadataList(List<WriterFileSet> fileMetadataList) {
            this.fileMetadataList = new ArrayList<>(fileMetadataList);
            return this;
        }

        /**
         * Adds a writer file set to merge.
         *
         * @param writerFileSet the writer file set to add
         * @return this builder
         */
        public Builder addFileMetadata(WriterFileSet writerFileSet) {
            this.fileMetadataList.add(writerFileSet);
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
         * Builds an immutable {@link MergeInput}.
         *
         * @return the constructed MergeInput
         */
        public MergeInput build() {
            return new MergeInput(this);
        }
    }
}
