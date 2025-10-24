/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * A reusable class to encode field → {extension → file size} mappings for segment statistics.
 * This class provides per-field disk consumption breakdown by file extension.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public final class FieldFileStats implements Writeable, Iterable<Map.Entry<String, Map<String, Long>>> {

    private final Map<String, Map<String, Long>> stats;

    /**
     * Creates a new FieldFileStats instance
     *
     * @param stats the field to extension to size mapping
     */
    public FieldFileStats(Map<String, Map<String, Long>> stats) {
        this.stats = Objects.requireNonNull(stats, "stats must be non-null");
        assert stats.containsKey(null) == false : "stats cannot contain null field names";
    }

    /**
     * Creates a new FieldFileStats instance from a stream
     *
     * @param input the stream input
     * @throws IOException if an I/O error occurs
     */
    public FieldFileStats(StreamInput input) throws IOException {
        int fieldCount = input.readVInt();
        this.stats = new HashMap<>(fieldCount);

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = input.readString();
            int extensionCount = input.readVInt();
            Map<String, Long> extensionSizes = new HashMap<>(extensionCount);

            for (int j = 0; j < extensionCount; j++) {
                String extension = input.readString();
                long size = input.readVLong();
                extensionSizes.put(extension, size);
            }

            stats.put(fieldName, extensionSizes);
        }
    }

    /**
     * Adds / merges the given field file stats into this stats instance
     *
     * @param fieldFileStats the stats to merge
     */
    public void add(FieldFileStats fieldFileStats) {
        for (final var fieldEntry : fieldFileStats.stats.entrySet()) {
            String fieldName = fieldEntry.getKey();
            Map<String, Long> otherExtensions = fieldEntry.getValue();

            stats.compute(fieldName, (k, existingExtensions) -> {
                if (existingExtensions == null) {
                    return new HashMap<>(otherExtensions);
                } else {
                    for (Map.Entry<String, Long> extEntry : otherExtensions.entrySet()) {
                        existingExtensions.merge(extEntry.getKey(), extEntry.getValue(), Long::sum);
                    }
                    return existingExtensions;
                }
            });
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(stats.size());

        for (Map.Entry<String, Map<String, Long>> fieldEntry : stats.entrySet()) {
            out.writeString(fieldEntry.getKey());
            Map<String, Long> extensionSizes = fieldEntry.getValue();
            out.writeVInt(extensionSizes.size());

            for (Map.Entry<String, Long> extEntry : extensionSizes.entrySet()) {
                out.writeString(extEntry.getKey());
                out.writeVLong(extEntry.getValue());
            }
        }
    }

    /**
     * Generates x-content into the given builder for field-level file statistics.
     * Output format:
     * <pre>
     * "field_level_file_sizes": {
     *   "fieldName": {
     *     "extension": {
     *       "size_in_bytes": 12345,
     *       "size": "12kb"
     *     }
     *   }
     * }
     * </pre>
     *
     * @param builder the builder to generate on
     * @param key the top level key for this stats object
     * @throws IOException if an I/O error occurs
     */
    public void toXContent(XContentBuilder builder, String key) throws IOException {
        builder.startObject(key);
        for (final var fieldEntry : stats.entrySet()) {
            builder.startObject(fieldEntry.getKey());
            for (final var extEntry : fieldEntry.getValue().entrySet()) {
                builder.startObject(extEntry.getKey());
                builder.humanReadableField("size_in_bytes", "size", new ByteSizeValue(extEntry.getValue()));
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
    }

    /**
     * Creates a deep copy of this stats instance
     *
     * @return a new copy of this FieldFileStats
     */
    public FieldFileStats copy() {
        Map<String, Map<String, Long>> copiedStats = new HashMap<>(stats.size());
        for (Map.Entry<String, Map<String, Long>> entry : stats.entrySet()) {
            copiedStats.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }
        return new FieldFileStats(copiedStats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldFileStats that = (FieldFileStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    @Override
    public Iterator<Map.Entry<String, Map<String, Long>>> iterator() {
        return stats.entrySet().iterator();
    }

    /**
     * Returns the file size in bytes for a specific field and extension,
     * or 0 if the field/extension combination is not present
     *
     * @param field the field name
     * @param extension the file extension
     * @return the size in bytes
     */
    public long get(String field, String extension) {
        Map<String, Long> extensions = stats.get(field);
        return extensions != null ? extensions.getOrDefault(extension, 0L) : 0L;
    }

    /**
     * Returns all extension sizes for a given field
     *
     * @param field the field name
     * @return map of extension to size, or null if field not present
     */
    public Map<String, Long> getField(String field) {
        return stats.get(field);
    }

    /**
     * Returns true if the given field is in the stats
     *
     * @param field the field name
     * @return true if field exists
     */
    public boolean containsField(String field) {
        return stats.containsKey(field);
    }

    /**
     * Returns the total number of fields tracked
     *
     * @return the number of fields
     */
    public int getFieldCount() {
        return stats.size();
    }
}
