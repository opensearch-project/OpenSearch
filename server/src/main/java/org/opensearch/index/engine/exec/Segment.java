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
     * @param version version with which this was serialized
     */
    public Segment(StreamInput in, Function<String, String> directoryResolver, long version) throws IOException {
        this(in.readLong(), readWriterFileSets(in, directoryResolver, version));
    }

    private static Map<String, WriterFileSet> readWriterFileSets(StreamInput in, Function<String, String> directoryResolver, long version)
        throws IOException {
        int size = in.readVInt();
        Map<String, WriterFileSet> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            map.put(key, new WriterFileSet(in, directoryResolver.apply(key), version));
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

    /**
     * Returns whether this segment holds only {@linkplain DataFormat#isAuxiliaryFormatName auxiliary}
     * formats — i.e. it is a side table (such as the nested child table) whose rows are elements
     * rather than documents.
     *
     * <p>Such a segment must be excluded wherever a shard's document count is derived from its
     * segments, because its {@code numRows} counts something else entirely. An auxiliary table
     * always occupies a segment of its own: {@code CatalogSnapshotManager} asserts that every
     * format within one segment reports the same row count, which a 2-document parent and its
     * 3-element child could not both satisfy.
     *
     * <p>An empty segment is not auxiliary — callers already treat "no files" separately.
     *
     * @return true when every format in this segment is auxiliary
     */
    public boolean isAuxiliaryOnly() {
        return dfGroupedSearchableFiles.isEmpty() == false
            && dfGroupedSearchableFiles.keySet().stream().allMatch(DataFormat::isAuxiliaryFormatName);
    }

    @Override
    public String toString() {
        return "Segment{" + "generation=" + generation + ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles + '}';
    }
}
