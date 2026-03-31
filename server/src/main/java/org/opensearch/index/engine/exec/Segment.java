/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a segment in the catalog snapshot containing files grouped by data format.
 * Each segment has a unique generation number and maintains searchable files organized by their data format type.
 */
@ExperimentalApi
public record Segment(long generation, Map<String, WriterFileSet> dfGroupedSearchableFiles) implements Writeable {

    public Segment {
        dfGroupedSearchableFiles = Map.copyOf(dfGroupedSearchableFiles);
    }

    /**
     * Constructs a Segment by deserializing from a {@link StreamInput}.
     */
    public Segment(StreamInput in) throws IOException {
        this(in.readLong(), in.readMap(StreamInput::readString, WriterFileSet::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(generation);
        out.writeVInt(dfGroupedSearchableFiles.size());
        for (Map.Entry<String, WriterFileSet> entry : dfGroupedSearchableFiles.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
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
