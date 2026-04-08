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

import java.util.HashMap;
import java.util.Map;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public record Segment(long generation, Map<String, WriterFileSet> dfGroupedSearchableFiles) {

    public static Builder builder(long generation) {
        return new Builder(generation);
    }

    public static final class Builder {
        private final long generation;
        private final Map<String, WriterFileSet> files = new HashMap<>();

        Builder(long generation) {
            this.generation = generation;
        }

        public Builder addSearchableFiles(DataFormat format, WriterFileSet writerFileSet) {
            files.put(format.name(), writerFileSet);
            return this;
        }

        public Segment build() {
            return new Segment(generation, Map.copyOf(files));
        }
    }
}
