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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A reusable class to encode {@code field -> (statName -> long)} mappings, with ordered stats.
 *
 * Each field holds a full set of stats named by {@code orderedStatNames}. Stats marked as "memory-like"
 * are rendered with humanReadableField using {@code memoryReadableKeys.get(statName)}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class FieldStats implements Writeable, Iterable<Map.Entry<String, Map<String, Long>>> {

    /** Ordered list of stat names (determines XContent order). */
    private final List<String> orderedStatNames;

    /** For each stat name, whether it should be rendered as a {@link ByteSizeValue} (humanReadableField). */
    private final Map<String, Boolean> isMemoryStat;

    /** For each memory stat name, the human-readable key to pair with its standard key in XContent. */
    private final Map<String, String> memoryReadableKeys;

    /** The data: field -> (statName -> value). Every field has entries for all stat names. */
    private final Map<String, Map<String, Long>> statsByField;

    /**
     * Create a new FieldStats.
     *
     * @param orderedStatNames ordered stat names
     * @param isMemoryStat mapping statName -> true if it should be displayed as a memory size
     * @param memoryReadableKeys mapping statName -> readableKey (required only when isMemoryStat is true for that stat)
     * @param statsByField the data
     */
    public FieldStats(
        List<String> orderedStatNames,
        Map<String, Boolean> isMemoryStat,
        Map<String, String> memoryReadableKeys,
        Map<String, Map<String, Long>> statsByField
    ) {
        assert orderedStatNames.stream().noneMatch(Objects::isNull) : "orderedStatNames cannot contain null values";
        this.orderedStatNames = orderedStatNames;

        Set<String> statNamesSet = new HashSet<>(orderedStatNames);
        for (String field : statsByField.keySet()) {
            assert statsByField.get(field).keySet().equals(statNamesSet);
            assert statsByField.get(field).values().stream().noneMatch(Objects::isNull) : "statsByField cannot contain null values";
        }
        this.statsByField = statsByField;

        assert isMemoryStat.keySet().equals(statNamesSet);
        assert isMemoryStat.values().stream().noneMatch(Objects::isNull) : "isMemoryStat cannot contain null values";
        this.isMemoryStat = isMemoryStat;

        validateReadableKeys(orderedStatNames, this.isMemoryStat, memoryReadableKeys);
        this.memoryReadableKeys = memoryReadableKeys;
    }

    public FieldStats(StreamInput in) throws IOException {
        this.orderedStatNames = Arrays.asList(in.readStringArray());
        this.isMemoryStat = in.readMap(StreamInput::readString, StreamInput::readBoolean);
        this.memoryReadableKeys = in.readMap(StreamInput::readString, StreamInput::readString);
        this.statsByField = new HashMap<>();
        int numEntries = in.readVInt();
        for (int i = 0; i < numEntries; i++) {
            String field = in.readString();
            Map<String, Long> values = in.readMap(StreamInput::readString, StreamInput::readLong);
            this.statsByField.put(field, values);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(orderedStatNames.toArray(new String[0]));
        out.writeMap(isMemoryStat, StreamOutput::writeString, StreamOutput::writeBoolean);
        out.writeMap(memoryReadableKeys, StreamOutput::writeString, StreamOutput::writeString);
        out.writeVInt(statsByField.size());
        for (Map.Entry<String, Map<String, Long>> entry : statsByField.entrySet()) {
            out.writeString(entry.getKey());
            out.writeMap(entry.getValue(), StreamOutput::writeString, StreamOutput::writeLong);
        }
    }

    private void validateReadableKeys(List<String> names, Map<String, Boolean> isMemory, Map<String, String> readable) {
        Objects.requireNonNull(readable, "memoryReadableKeys must be non-null");
        for (String stat : names) {
            if (isMemory.get(stat)) {
                assert readable.get(stat) != null : "memoryReadableKeys missing readable key for memory stat: " + stat;
            }
        }
    }

    /** Merge another FieldStats into this object (must have same stat shape). */
    public void add(FieldStats other) {
        requireSameShape(other);
        for (Map.Entry<String, Map<String, Long>> e : other.statsByField.entrySet()) {
            Map<String, Long> target = statsByField.computeIfAbsent(e.getKey(), k -> zeroedStats());
            Map<String, Long> src = e.getValue();
            for (String stat : orderedStatNames) {
                target.put(stat, target.get(stat) + src.get(stat));
            }
        }
    }

    public Long get(String fieldName, String statName) {
        Map<String, Long> perFieldData = statsByField.get(fieldName);
        if (perFieldData == null) return null;
        return perFieldData.get(statName);
    }

    public Set<String> getFieldNames() {
        return statsByField.keySet();
    }

    private void requireSameShape(FieldStats other) {
        if (!this.orderedStatNames.equals(other.orderedStatNames)) {
            throw new IllegalArgumentException("Mismatched stat ordering/names");
        }
        if (!this.isMemoryStat.equals(other.isMemoryStat)) {
            throw new IllegalArgumentException("Mismatched isMemoryStat");
        }
        if (!this.memoryReadableKeys.equals(other.memoryReadableKeys)) {
            // Allow non-memory keys to be missing, but memory ones must match
            for (String stat : orderedStatNames) {
                if (isMemoryStat.get(stat)) {
                    if (!Objects.equals(memoryReadableKeys.get(stat), other.memoryReadableKeys.get(stat))) {
                        throw new IllegalArgumentException("Mismatched readable key for memory stat: " + stat);
                    }
                }
            }
        }
    }

    private Map<String, Long> zeroedStats() {
        Map<String, Long> m = new HashMap<>(orderedStatNames.size());
        for (String stat : orderedStatNames) {
            m.put(stat, 0L);
        }
        return m;
    }

    public void toXContent(XContentBuilder builder, String key) throws IOException {
        builder.startObject(key);
        for (Map.Entry<String, Map<String, Long>> fieldEntry : statsByField.entrySet()) {
            builder.startObject(fieldEntry.getKey());
            Map<String, Long> per = fieldEntry.getValue();
            for (String stat : orderedStatNames) {
                long v = per.getOrDefault(stat, 0L);
                if (isMemoryStat.get(stat)) {
                    String readableKey = memoryReadableKeys.get(stat);
                    // Use humanReadableField(standardKey, readableKey, ByteSizeValue)
                    builder.humanReadableField(stat, readableKey, new ByteSizeValue(v));
                } else {
                    builder.field(stat, v);
                }
            }
            builder.endObject();
        }
        builder.endObject();
    }

    @Override
    public Iterator<Map.Entry<String, Map<String, Long>>> iterator() {
        return statsByField.entrySet().iterator();
    }

    /** Deep copy. */
    public FieldStats copy() {
        Map<String, Map<String, Long>> cloned = new HashMap<>(statsByField.size());
        for (Map.Entry<String, Map<String, Long>> e : statsByField.entrySet()) {
            cloned.put(e.getKey(), new HashMap<>(e.getValue()));
        }
        return new FieldStats(orderedStatNames, isMemoryStat, memoryReadableKeys, cloned);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldStats that = (FieldStats) o;
        return Objects.equals(orderedStatNames, that.orderedStatNames)
            && Objects.equals(isMemoryStat, that.isMemoryStat)
            && Objects.equals(memoryReadableKeys, that.memoryReadableKeys)
            && Objects.equals(statsByField, that.statsByField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderedStatNames, isMemoryStat, memoryReadableKeys, statsByField);
    }

    /**
     * Convenience method for converting one stat's values to FieldMemoryStats.
     */
    public FieldMemoryStats toFieldMemoryStats(String statName) {
        if (!orderedStatNames.contains(statName)) {
            throw new IllegalArgumentException("Unknown stat: " + statName);
        }
        final Map<String, Long> flat = new HashMap<>(statsByField.size());
        for (Map.Entry<String, Map<String, Long>> e : statsByField.entrySet()) {
            final Long v = e.getValue().get(statName);
            assert v != null : "Missing value for stat '" + statName + "' in field '" + e.getKey() + "'";
            flat.put(e.getKey(), v);
        }
        return new FieldMemoryStats(flat);
    }

    /**
     * Build a FieldStats from a single FieldMemoryStats.
     * All other stats in {@code orderedStatNames} are initialized to 0L.
     */
    public static FieldStats fromSingleStat(
        final List<String> orderedStatNames,
        final Map<String, Boolean> isMemoryStat,
        final Map<String, String> memoryReadableKeys,
        final FieldMemoryStats singleStat,
        final String statName
    ) {
        Objects.requireNonNull(orderedStatNames, "orderedStatNames");
        Objects.requireNonNull(isMemoryStat, "isMemoryStat");
        Objects.requireNonNull(memoryReadableKeys, "memoryReadableKeys");
        Objects.requireNonNull(singleStat, "singleStat");
        Objects.requireNonNull(statName, "statName");

        if (orderedStatNames.contains(statName) == false) {
            throw new IllegalArgumentException("Unknown stat: " + statName);
        }

        final Map<String, Map<String, Long>> statsByField = new HashMap<>();
        for (Map.Entry<String, Long> e : singleStat) { // FieldMemoryStats is Iterable<Entry<String,Long>>
            final String field = e.getKey();
            final long value = e.getValue();
            final Map<String, Long> dataForField = new HashMap<>();
            for (String s : orderedStatNames) {
                dataForField.put(s, 0L);
            }
            dataForField.put(statName, value);
            statsByField.put(field, dataForField);
        }
        return new FieldStats(orderedStatNames, isMemoryStat, memoryReadableKeys, statsByField);
    }
}
