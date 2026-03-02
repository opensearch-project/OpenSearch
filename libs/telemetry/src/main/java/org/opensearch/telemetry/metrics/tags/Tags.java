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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable tag container for metric dimensions, backed by sorted parallel arrays
 * with a precomputed hash. Safe to store in fields, share across threads, and use
 * as map keys without defensive copies.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class Tags {

    /** Empty tags singleton. */
    public static final Tags EMPTY = new Tags(new String[0], new Object[0], computeHash(new String[0], new Object[0]));

    private final String[] keys;
    private final Object[] values;
    private final int hashCode;

    private Tags(String[] keys, Object[] values, int hashCode) {
        this.keys = keys;
        this.values = values;
        this.hashCode = hashCode;
    }

    // -----------------------------------------------------------------------
    // Immutable factories
    // -----------------------------------------------------------------------

    /**
     * Creates a single-tag instance.
     * @param key tag key
     * @param value tag value
     * @return immutable Tags
     */
    public static Tags of(String key, Object value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        String[] k = { key };
        Object[] v = { value };
        return new Tags(k, v, computeHash(k, v));
    }

    /**
     * Creates a two-tag instance.
     * @param k1 first key
     * @param v1 first value
     * @param k2 second key
     * @param v2 second value
     * @return immutable Tags
     */
    public static Tags of(String k1, Object v1, String k2, Object v2) {
        Objects.requireNonNull(k1, "k1 must not be null");
        Objects.requireNonNull(v1, "v1 must not be null");
        Objects.requireNonNull(k2, "k2 must not be null");
        Objects.requireNonNull(v2, "v2 must not be null");
        int cmp = k1.compareTo(k2);
        String[] keys;
        Object[] values;
        if (cmp < 0) {
            keys = new String[] { k1, k2 };
            values = new Object[] { v1, v2 };
        } else if (cmp > 0) {
            keys = new String[] { k2, k1 };
            values = new Object[] { v2, v1 };
        } else {
            keys = new String[] { k2 };
            values = new Object[] { v2 };
        }
        return new Tags(keys, values, computeHash(keys, values));
    }

    /**
     * Creates a three-tag instance.
     * @param k1 first key
     * @param v1 first value
     * @param k2 second key
     * @param v2 second value
     * @param k3 third key
     * @param v3 third value
     * @return immutable Tags
     */
    public static Tags of(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        Objects.requireNonNull(k1, "k1 must not be null");
        Objects.requireNonNull(v1, "v1 must not be null");
        Objects.requireNonNull(k2, "k2 must not be null");
        Objects.requireNonNull(v2, "v2 must not be null");
        Objects.requireNonNull(k3, "k3 must not be null");
        Objects.requireNonNull(v3, "v3 must not be null");
        return fromPairs(new String[] { k1, k2, k3 }, new Object[] { v1, v2, v3 }, 3);
    }

    /**
     * Creates a four-tag instance.
     * @param k1 first key
     * @param v1 first value
     * @param k2 second key
     * @param v2 second value
     * @param k3 third key
     * @param v3 third value
     * @param k4 fourth key
     * @param v4 fourth value
     * @return immutable Tags
     */
    public static Tags of(String k1, Object v1, String k2, Object v2,
                           String k3, Object v3, String k4, Object v4) {
        Objects.requireNonNull(k1, "k1 must not be null");
        Objects.requireNonNull(v1, "v1 must not be null");
        Objects.requireNonNull(k2, "k2 must not be null");
        Objects.requireNonNull(v2, "v2 must not be null");
        Objects.requireNonNull(k3, "k3 must not be null");
        Objects.requireNonNull(v3, "v3 must not be null");
        Objects.requireNonNull(k4, "k4 must not be null");
        Objects.requireNonNull(v4, "v4 must not be null");
        return fromPairs(new String[] { k1, k2, k3, k4 }, new Object[] { v1, v2, v3, v4 }, 4);
    }

    /**
     * Creates Tags from interleaved key-value pairs.
     * @param keyValues interleaved key-value pairs (String values only); must be even length
     * @return immutable Tags
     */
    public static Tags of(String... keyValues) {
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
     * Merges two Tags via merge-sort on their sorted arrays. On key collision, {@code b} wins.
     * @param a first tags (may be null)
     * @param b second tags (may be null); values take precedence on key collision
     * @return merged immutable Tags
     */
    public static Tags concat(Tags a, Tags b) {
        if (a == null || a.keys.length == 0) return (b != null) ? b : EMPTY;
        if (b == null || b.keys.length == 0) return a;

        int aLen = a.keys.length, bLen = b.keys.length;
        String[] mk = new String[aLen + bLen];
        Object[] mv = new Object[aLen + bLen];
        int ai = 0, bi = 0, w = 0;

        while (ai < aLen && bi < bLen) {
            int cmp = a.keys[ai].compareTo(b.keys[bi]);
            if (cmp < 0) {
                mk[w] = a.keys[ai];
                mv[w] = a.values[ai];
                ai++;
            } else if (cmp > 0) {
                mk[w] = b.keys[bi];
                mv[w] = b.values[bi];
                bi++;
            } else {
                mk[w] = b.keys[bi];
                mv[w] = b.values[bi];
                ai++;
                bi++;
            }
            w++;
        }
        while (ai < aLen) { mk[w] = a.keys[ai]; mv[w] = a.values[ai]; ai++; w++; }
        while (bi < bLen) { mk[w] = b.keys[bi]; mv[w] = b.values[bi]; bi++; w++; }

        String[] keys = (w == mk.length) ? mk : Arrays.copyOf(mk, w);
        Object[] values = (w == mv.length) ? mv : Arrays.copyOf(mv, w);
        return new Tags(keys, values, computeHash(keys, values));
    }

    /**
     * Creates Tags from a {@code Map<String, String>}.
     * @param map tag key-value pairs
     * @return immutable Tags
     */
    public static Tags fromMap(Map<String, String> map) {
        if (map == null || map.isEmpty()) return EMPTY;
        String[] keys = map.keySet().toArray(new String[0]);
        Arrays.sort(keys);
        Object[] values = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            values[i] = map.get(keys[i]);
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
     * Converts all values to String. Intended for flush-time, not the hot path.
     * @return mutable map of String keys to String values
     */
    public Map<String, String> toMap() {
        if (keys.length == 0) return Collections.emptyMap();
        Map<String, String> map = new HashMap<>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], String.valueOf(values[i]));
        }
        return map;
    }

    /**
     * Returns an unmodifiable map preserving original value types.
     * @return unmodifiable map of tags
     */
    public Map<String, ?> getTagsMap() {
        if (keys.length == 0) return Collections.emptyMap();
        Map<String, Object> map = new HashMap<>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }
        return Collections.unmodifiableMap(map);
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
        if (this.keys.length != that.keys.length) return false;
        for (int i = 0; i < keys.length; i++) {
            if (!keys[i].equals(that.keys[i])) return false;
            if (!values[i].equals(that.values[i])) return false;
        }
        return true;
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

    // -----------------------------------------------------------------------
    // Deprecated API — returns new instances; fluent chains work unchanged.
    // -----------------------------------------------------------------------

    /**
     * Returns an empty Tags instance.
     * @return empty Tags
     * @deprecated Use {@link #EMPTY} instead.
     */
    @Deprecated
    public static Tags create() {
        return EMPTY;
    }

    /**
     * Returns a new Tags with the given String tag appended.
     * @param key tag key
     * @param value tag value
     * @return new Tags with the added tag
     * @deprecated Use {@link #of} or {@link #concat} instead.
     */
    @Deprecated
    public Tags addTag(String key, String value) {
        Objects.requireNonNull(value, "value cannot be null");
        return Tags.concat(this, Tags.of(key, (Object) value));
    }

    /**
     * Returns a new Tags with the given long tag appended.
     * @param key tag key
     * @param value tag value
     * @return new Tags with the added tag
     * @deprecated Use {@link #of} or {@link #concat} instead.
     */
    @Deprecated
    public Tags addTag(String key, long value) {
        return Tags.concat(this, Tags.of(key, (Object) value));
    }

    /**
     * Returns a new Tags with the given double tag appended.
     * @param key tag key
     * @param value tag value
     * @return new Tags with the added tag
     * @deprecated Use {@link #of} or {@link #concat} instead.
     */
    @Deprecated
    public Tags addTag(String key, double value) {
        return Tags.concat(this, Tags.of(key, (Object) value));
    }

    /**
     * Returns a new Tags with the given boolean tag appended.
     * @param key tag key
     * @param value tag value
     * @return new Tags with the added tag
     * @deprecated Use {@link #of} or {@link #concat} instead.
     */
    @Deprecated
    public Tags addTag(String key, boolean value) {
        return Tags.concat(this, Tags.of(key, (Object) value));
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

    /** Insertion-sorts by key, deduplicates (last value wins). */
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
