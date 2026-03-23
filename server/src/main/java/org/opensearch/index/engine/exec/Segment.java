/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a segment in the catalog snapshot containing files grouped by data format.
 * Each segment has a unique generation number and maintains searchable files organized by their data format type.
 * This class is serializable and can be transmitted across nodes for replication and recovery operations.
 */
@ExperimentalApi
public record Segment(long generation, Map<String, WriterFileSet> dfGroupedSearchableFiles) {

    public Segment {
        dfGroupedSearchableFiles = Map.copyOf(dfGroupedSearchableFiles);
    }

    public static Builder builder(long generation) {
        return new Builder(generation);
    }

    /**
     * Builder for {@link Segment}.
     */
    @ExperimentalApi
    public static class Builder {
        private final long generation;
        private final Map<String, WriterFileSet> dfGroupedSearchableFiles = new HashMap<>();

        private Builder(long generation) {
            this.generation = generation;
        }

        public Builder addSearchableFiles(DataFormat dataFormat, WriterFileSet writerFileSetGroup) {
            dfGroupedSearchableFiles.put(dataFormat.name(), writerFileSetGroup);
            return this;
        }

        public Segment build() {
            return new Segment(generation, dfGroupedSearchableFiles);
        }
    }
}
