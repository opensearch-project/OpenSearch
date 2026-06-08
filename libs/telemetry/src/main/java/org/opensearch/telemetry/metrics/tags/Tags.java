/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics.tags;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable tags for a meter.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class Tags {

    private static final String[] EMPTY_KEYS = new String[0];
    private static final Object[] EMPTY_VALUES = new Object[0];

    /**
     * Empty tags singleton.
     */
    public static final Tags EMPTY = new Tags(EMPTY_KEYS, EMPTY_VALUES, 1);

    private final String[] keys;
    private final Object[] values;
    private final int hashCode;

    private Tags(String[] keys, Object[] values, int hashCode) {
        this.keys = keys;
        this.values = values;
        this.hashCode = hashCode;
    }

    // -----------------------------------------------------------------------
    // Factories
    // -----------------------------------------------------------------------

    /**
     * Creates an immutable Tags with one String-valued pair.
     * @param key   tag key
     * @param value tag value
     * @return new Tags instance
     */
    public static Tags of(String key, String value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        String[] k = { key };
        Object[] v = { value };
        return new Tags(k, v, computeHash(k, v));
    }

    /**
     * Creates an immutable Tags with one long-valued pair.
     * @param key   tag key
     * @param value tag value
     * @return new Tags instance
     */
    public static Tags of(String key, long value) {
        Objects.requireNonNull(key, "key must not be null");
        String[] k = { key };
        Object[] v = { value };
        return new Tags(k, v, computeHash(k, v));
    }

    /**
     * Creates an immutable Tags with one double-valued pair.
     * @param key   tag key
     * @param value tag value
     * @return new Tags instance
     */
    public static Tags of(String key, double value) {
        Objects.requireNonNull(key, "key must not be null");
        String[] k = { key };
        Object[] v = { value };
        return new Tags(k, v, computeHash(k, v));
    }

    /**
     * Creates an immutable Tags with one boolean-valued pair.
     * @param key   tag key
     * @param value tag value
     * @return new Tags instance
     */
    public static Tags of(String key, boolean value) {
        Objects.requireNonNull(key, "key must not be null");
        String[] k = { key };
        Object[] v = { value };
        return new Tags(k, v, computeHash(k, v));
    }

    /**
     * Creates Tags from interleaved String key-value pairs; must be even length.
     * @param keyValues alternating keys and values
     * @return new Tags instance
     */
    public static Tags ofStringPairs(String... keyValues) {
        if (keyValues == null || keyValues.length == 0) return EMPTY;
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("keyValues must be even length, got " + keyValues.length);
        }
        int count = keyValues.length / 2;
        String[] keys = new String[count];
        Object[] values = new Object[count];
        for (int i = 0; i < count; i++) {
            keys[i] = Objects.requireNonNull(keyValues[i * 2], "key at index " + (i * 2) + " must not be null");
            values[i] = Objects.requireNonNull(keyValues[i * 2 + 1], "value at index " + (i * 2 + 1) + " must not be null");
        }
        return fromPairs(keys, values, count);
    }

    /**
     * Merges two Tags. On key collision, {@code b} wins. Either argument may be null.
     * @param a first tags
     * @param b second tags
     * @return merged Tags instance
     */
    public static Tags concat(Tags a, Tags b) {
        if (a == null || a.keys.length == 0) return (b != null) ? b : EMPTY;
        if (b == null || b.keys.length == 0) return a;

        int thisLength = a.keys.length;
        int otherLength = b.keys.length;
        String[] mergedKeys = new String[thisLength + otherLength];
        Object[] mergedValues = new Object[thisLength + otherLength];
        int thisIndex = 0, otherIndex = 0, sortedIndex = 0;

        while (thisIndex < thisLength && otherIndex < otherLength) {
            int cmp = a.keys[thisIndex].compareTo(b.keys[otherIndex]);
            if (cmp < 0) {
                mergedKeys[sortedIndex] = a.keys[thisIndex];
                mergedValues[sortedIndex] = a.values[thisIndex];
                thisIndex++;
            } else if (cmp > 0) {
                mergedKeys[sortedIndex] = b.keys[otherIndex];
                mergedValues[sortedIndex] = b.values[otherIndex];
                otherIndex++;
            } else {
                mergedKeys[sortedIndex] = b.keys[otherIndex];
                mergedValues[sortedIndex] = b.values[otherIndex];
                thisIndex++;
                otherIndex++;
            }
            sortedIndex++;
        }
        int thisRemaining = thisLength - thisIndex;
        if (thisRemaining > 0) {
            System.arraycopy(a.keys, thisIndex, mergedKeys, sortedIndex, thisRemaining);
            System.arraycopy(a.values, thisIndex, mergedValues, sortedIndex, thisRemaining);
            sortedIndex += thisRemaining;
        }
        int otherRemaining = otherLength - otherIndex;
        if (otherRemaining > 0) {
            System.arraycopy(b.keys, otherIndex, mergedKeys, sortedIndex, otherRemaining);
            System.arraycopy(b.values, otherIndex, mergedValues, sortedIndex, otherRemaining);
            sortedIndex += otherRemaining;
        }

        String[] keys = (sortedIndex == mergedKeys.length) ? mergedKeys : Arrays.copyOf(mergedKeys, sortedIndex);
        Object[] values = (sortedIndex == mergedValues.length) ? mergedValues : Arrays.copyOf(mergedValues, sortedIndex);
        return new Tags(keys, values, computeHash(keys, values));
    }

    /**
     * Creates Tags from a map.
     * @param map key-value pairs
     * @return new Tags instance
     */
    public static Tags fromMap(Map<String, ?> map) {
        if (map == null || map.isEmpty()) return EMPTY;
        String[] keys = map.keySet().toArray(new String[0]);
        Arrays.sort(keys);
        Object[] values = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            values[i] = Objects.requireNonNull(map.get(keys[i]), "value for key '" + keys[i] + "' must not be null");
        }
        return new Tags(keys, values, computeHash(keys, values));
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /**
     * Returns the number of tags.
     * @return tag count
     */
    public int size() {
        return keys.length;
    }

    /**
     * Returns the key at the given index.
     * @param i index
     * @return key
     */
    public String getKey(int i) {
        return keys[i];
    }

    /**
     * Returns the value at the given index.
     * @param i index
     * @return value
     */
    public Object getValue(int i) {
        return values[i];
    }

    /**
     * Returns an unmodifiable map preserving original value types.
     * @return unmodifiable map of tags
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<String, ?> getTagsMap() {
        if (keys.length == 0) return Map.of();
        Map.Entry<String, ?>[] entries = new Map.Entry[keys.length];
        for (int i = 0; i < keys.length; i++) {
            entries[i] = Map.entry(keys[i], values[i]);
        }
        return Map.ofEntries(entries);
    }

    // -----------------------------------------------------------------------
    // equals / hashCode / toString
    // -----------------------------------------------------------------------

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tags that)) return false;
        if (this.hashCode != that.hashCode) return false;
        return Arrays.equals(keys, that.keys) && Arrays.equals(values, that.values);
    }

    @Override
    public String toString() {
        if (keys.length == 0) return "Tags{}";
        StringBuilder sb = new StringBuilder("Tags{");
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(keys[i]).append('=').append(values[i]);
        }
        return sb.append('}').toString();
    }

    /**
     * Factory method.
     * @return empty tags
     */
    public static Tags create() {
        return EMPTY;
    }

    /**
     * Add String attribute.
     * @param key   key
     * @param value value
     * @return new Tags instance with the added tag
     */
    public Tags addTag(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        return Tags.concat(this, Tags.of(key, value));
    }

    /**
     * Add long attribute.
     * @param key   key
     * @param value value
     * @return new Tags instance with the added tag
     */
    public Tags addTag(String key, long value) {
        Objects.requireNonNull(key, "key cannot be null");
        return Tags.concat(this, Tags.of(key, value));
    }

    /**
     * Add double attribute.
     * @param key   key
     * @param value value
     * @return new Tags instance with the added tag
     */
    public Tags addTag(String key, double value) {
        Objects.requireNonNull(key, "key cannot be null");
        return Tags.concat(this, Tags.of(key, value));
    }

    /**
     * Add boolean attribute.
     * @param key   key
     * @param value value
     * @return new Tags instance with the added tag
     */
    public Tags addTag(String key, boolean value) {
        Objects.requireNonNull(key, "key cannot be null");
        return Tags.concat(this, Tags.of(key, value));
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private static int computeHash(String[] keys, Object[] values) {
        int result = 1;
        for (int i = 0; i < keys.length; i++) {
            result = 31 * result + keys[i].hashCode();
            result = 31 * result + values[i].hashCode();
        }
        return result;
    }

    /** Insertion-sorts by key, deduplicates (last value wins). Mutates the provided arrays. */
    private static Tags fromPairs(String[] rawKeys, Object[] rawValues, int count) {
        if (count == 0) return EMPTY;

        for (int i = 1; i < count; i++) {
            String key = rawKeys[i];
            Object val = rawValues[i];
            int j = i - 1;
            while (j >= 0 && rawKeys[j].compareTo(key) > 0) {
                rawKeys[j + 1] = rawKeys[j];
                rawValues[j + 1] = rawValues[j];
                j--;
            }
            rawKeys[j + 1] = key;
            rawValues[j + 1] = val;
        }

        int w = 0;
        for (int i = 0; i < count; i++) {
            if (w > 0 && rawKeys[w - 1].equals(rawKeys[i])) {
                rawValues[w - 1] = rawValues[i];
            } else {
                rawKeys[w] = rawKeys[i];
                rawValues[w] = rawValues[i];
                w++;
            }
        }

        String[] keys = (w == count) ? rawKeys : Arrays.copyOf(rawKeys, w);
        Object[] values = (w == count) ? rawValues : Arrays.copyOf(rawValues, w);
        return new Tags(keys, values, computeHash(keys, values));
    }
}
