/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class FieldStats implements Writeable, Iterable<Map.Entry<String, Long>> {
    protected final Map<String, Long> stats;

    /**
     * Creates a new FieldMemoryStats instance
     */
    public FieldStats(Map<String, Long> stats) {
        this.stats = Objects.requireNonNull(stats, "status must be non-null");
        assert stats.containsKey(null) == false;
    }

    /**
     * Creates a new FieldMemoryStats instance from a stream
     */
    public FieldStats(StreamInput input) throws IOException {
        stats = input.readMap(StreamInput::readString, StreamInput::readVLong);
    }

    /**
     * Adds / merges the given field memory stats into this stats instance
     */
    public void add(FieldStats fieldStats) {
        for (final var entry : fieldStats.stats.entrySet()) {
            stats.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(stats, StreamOutput::writeString, StreamOutput::writeVLong);
    }

    public Map<String, Long> getStats() {
        return stats;
    }

    /**
     * Creates a deep copy of this stats instance
     */
    public abstract FieldStats copy();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMemoryStats that = (FieldMemoryStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    @Override
    public Iterator<Map.Entry<String, Long>> iterator() {
        return stats.entrySet().iterator();
    }

    /**
     * Returns the fields value in bytes or <code>0</code> if it's not present in the stats
     */
    public long get(String field) {
        return stats.getOrDefault(field, 0L);
    }

    /**
     * Returns <code>true</code> iff the given field is in the stats
     */
    public boolean containsField(String field) {
        return stats.containsKey(field);
    }

    /**
     * Generates x-content into the given builder for each of the fields in this stats instance
     * @param builder the builder to generated on
     * @param key the top level key for this stats object
     * @param rawKey the raw byte key for each of the fields byte sizes
     * @param readableKey the readable key for each of the fields byte sizes
     */
    public abstract void toXContent(XContentBuilder builder, String key, String rawKey, String readableKey) throws IOException;

    /**
     * Convenience method for adding XContent for a single field to the builder.
     */
    public abstract void toXContentField(XContentBuilder builder, String field, String rawKey, String readableKey) throws IOException;

    /**
     * Convenience method for converting multiple FieldStats to XContent, such that the top-level grouping
     * is the field names rather than the different FieldStats objects.
     * @param builder the builder
     * @param fieldNames the field names all stats objects share
     * @param statsList Ordered list of FieldMemoryStats objects. Can be null, in which case they are skipped. Field names for non-null stats must match the provided list.
     * @param rawKeys Ordered list of raw key values for each object. Should match the order of statsList.
     * @param readableKeys Ordered list of readable key values for each object. Should match the order of statsList.
     */
    public static void toXContent(
        XContentBuilder builder,
        Set<String> fieldNames,
        String topLevelKey,
        List<FieldStats> statsList,
        List<String> rawKeys,
        List<String> readableKeys
    ) throws IOException {
        for (FieldStats stats : statsList) {
            if (stats != null) {
                if (!stats.stats.keySet().equals(fieldNames)) {
                    throw new IllegalArgumentException("All provided stats must have field names matching the provided fieldNames set");
                }
            }
        }
        if (statsList.size() != rawKeys.size() || statsList.size() != readableKeys.size()) {
            throw new IllegalArgumentException("statsList, rawKeys, and readableKeys must have the same size");
        }

        builder.startObject(topLevelKey);
        for (String field : fieldNames) {
            builder.startObject(field);
            for (int i = 0; i < statsList.size(); i++) {
                FieldStats stats = statsList.get(i);
                String rawKey = rawKeys.get(i);
                String readableKey = readableKeys.get(i);
                if (stats != null) {
                    stats.toXContentField(builder, field, rawKey, readableKey);
                }
            }
            builder.endObject();
        }
        builder.endObject();
    }
}
