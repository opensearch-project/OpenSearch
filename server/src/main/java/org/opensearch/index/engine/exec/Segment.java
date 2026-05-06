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
import java.util.function.Function;

/**
 * Represents a segment in the catalog snapshot containing files grouped by data format.
 * Each segment has a unique generation number and maintains searchable files organized by their data format type.
 * This class is serializable and can be transmitted across nodes for replication and recovery operations.
 */
@ExperimentalApi
public record Segment(long generation, Map<String, WriterFileSet> dfGroupedSearchableFiles) implements Writeable {

    public Segment {
        dfGroupedSearchableFiles = Map.copyOf(dfGroupedSearchableFiles);
    }

    /**
     * Constructs a Segment by deserializing from a {@link StreamInput}.
     *
     * @param in the stream input to read from
     * @param directoryResolver function that maps a data format name to its directory path
     */
    public Segment(StreamInput in, Function<String, String> directoryResolver) throws IOException {
        this(in.readLong(), readWriterFileSets(in, directoryResolver));
    }

    private static Map<String, WriterFileSet> readWriterFileSets(StreamInput in, Function<String, String> directoryResolver)
        throws IOException {
        int size = in.readVInt();
        Map<String, WriterFileSet> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            map.put(key, new WriterFileSet(in, directoryResolver.apply(key)));
        }
        return map;
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

        public Builder addSearchableFiles(String dataFormatName, WriterFileSet writerFileSetGroup) {
            dfGroupedSearchableFiles.put(dataFormatName, writerFileSetGroup);
            return this;
        }

        public Segment build() {
            return new Segment(generation, dfGroupedSearchableFiles);
        }
    }

    /**
     * Stable identity string used by segment-replication machinery to name this segment.
     * Must remain equal across primary (publish) and replica (cleanup) for the same segment.
     */
    public String replicationCheckpointName() {
        return Long.toString(generation);
    }

    @Override
    public String toString() {
        return "Segment{" + "generation=" + generation + ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles + '}';
    }
}
