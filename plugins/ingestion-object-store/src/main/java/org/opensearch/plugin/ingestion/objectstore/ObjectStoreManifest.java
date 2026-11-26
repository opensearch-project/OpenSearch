/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * An immutable, ordered snapshot of {@code objectKey -> size} entries built once at index creation.
 * <p>
 * Entries are stored in UTF-8 unsigned-byte key order (the same order {@link ObjectStorePointer} imposes) via a
 * {@link TreeMap} so iteration and {@code latestPointer} are deterministic and consistent with the pointer's
 * order-preservation.
 */
final class ObjectStoreManifest {

    private final SortedMap<String, Long> objects;
    private final SortedSet<String> keys;

    /**
     * The single definition of object-key order for this source: UTF-8 unsigned byte order, matching
     * {@link ObjectStorePointer#compareTo}. Slice order must agree with pointer order, so both the manifest and the
     * {@code discovery=list} walk sort with this.
     */
    static final Comparator<String> BYTE_ORDER = (a, b) -> {
        int shared = Math.min(a.length(), b.length());
        for (int i = 0; i < shared; i++) {
            char left = a.charAt(i);
            char right = b.charAt(i);
            if (left != right) {
                return codePointRank(left) - codePointRank(right);
            }
        }
        return a.length() - b.length();
    };

    /**
     * Maps a UTF-16 code unit onto a value whose natural order matches UTF-8 byte order. Java compares chars by code
     * unit, which puts the surrogate range D800-DFFF below E000-FFFF, while UTF-8 puts it above; shifting both ranges
     * corrects that, so this comparison is allocation-free yet identical to comparing the encoded bytes.
     *
     * @param unit the code unit
     * @return the rank to compare by
     */
    private static int codePointRank(char unit) {
        if (unit < 0xD800) {
            return unit;
        }
        return unit < 0xE000 ? unit + 0x2000 : unit - 0x800;
    }

    /**
     * The manifest with no objects, used when the object set is discovered by listing rather than declared.
     */
    public static final ObjectStoreManifest EMPTY = new ObjectStoreManifest(Map.of());

    /**
     * Constructs an immutable manifest from the given {@code key -> size} map.
     *
     * @param objects the map of object keys to their sizes in bytes; must not be {@code null}
     * @throws NullPointerException     if the map or any key is {@code null}
     * @throws IllegalArgumentException if any size is negative
     */
    public ObjectStoreManifest(final Map<String, Long> objects) {
        Objects.requireNonNull(objects, "objects must not be null");
        final TreeMap<String, Long> sorted = new TreeMap<>(BYTE_ORDER);
        for (final Map.Entry<String, Long> entry : objects.entrySet()) {
            final String key = Objects.requireNonNull(entry.getKey(), "object key must not be null");
            requireWellFormedUtf16(key);
            final Long size = Objects.requireNonNull(entry.getValue(), "object size must not be null");
            if (size < 0L) {
                throw new IllegalArgumentException(
                    "object size for key [" + LogSanitizer.sanitize(key) + "] must be >= 0 but was [" + size + "]"
                );
            }
            sorted.put(key, size);
        }
        this.objects = Collections.unmodifiableSortedMap(sorted);
        this.keys = Collections.unmodifiableSortedSet(sorted.navigableKeySet());
    }

    /**
     * Rejects a key that is not well-formed UTF-16, meaning it carries an unpaired surrogate. Such a key would break
     * the guarantee that manifest order matches pointer order, because {@link ObjectStorePointer} encodes the key with
     * {@code getBytes(UTF_8)}, which substitutes a single {@code ?} for an unpaired surrogate. Two distinct keys would
     * then encode to identical pointer bytes, and a resumed pointer would decode to a key that is not in the manifest.
     *
     * @param key the object key
     * @throws IllegalArgumentException if the key contains an unpaired surrogate
     */
    private static void requireWellFormedUtf16(final String key) {
        for (int i = 0; i < key.length(); i++) {
            final char unit = key.charAt(i);
            if (Character.isHighSurrogate(unit)) {
                if (i + 1 == key.length() || Character.isLowSurrogate(key.charAt(i + 1)) == false) {
                    throw new IllegalArgumentException(
                        "object key [" + LogSanitizer.sanitize(key) + "] contains an unpaired surrogate and is not valid UTF-16"
                    );
                }
                i++;
            } else if (Character.isLowSurrogate(unit)) {
                throw new IllegalArgumentException(
                    "object key [" + LogSanitizer.sanitize(key) + "] contains an unpaired surrogate and is not valid UTF-16"
                );
            }
        }
    }

    /**
     * Returns the object keys in UTF-8 unsigned-byte order (the same order {@link ObjectStorePointer} imposes), with
     * a Java-string tiebreak so distinct keys are never conflated.
     *
     * @return an immutable, sorted view of the keys in UTF-8 byte order
     */
    public SortedSet<String> keys() {
        return keys;
    }

    /**
     * Returns the size in bytes of the object stored under the given key.
     *
     * @param key the object key
     * @return the size in bytes
     * @throws IllegalArgumentException if the key is not present in the manifest
     */
    public long sizeOf(final String key) {
        final Long size = objects.get(key);
        if (size == null) {
            throw new IllegalArgumentException("no object with key [" + LogSanitizer.sanitize(key) + "] in manifest");
        }
        return size;
    }

    /**
     * Returns the number of objects in the manifest.
     *
     * @return the object count
     */
    public int objectCount() {
        return objects.size();
    }

    /**
     * Tests whether the manifest contains an object under the given key.
     *
     * @param key the object key
     * @return {@code true} if the key is present, otherwise {@code false}
     */
    public boolean contains(final String key) {
        return objects.containsKey(key);
    }

    /**
     * Returns an unmodifiable, key-sorted view of the {@code key -> size} entries.
     *
     * @return the unmodifiable sorted map
     */
    public SortedMap<String, Long> asSortedMap() {
        return objects;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ObjectStoreManifest that = (ObjectStoreManifest) o;
        return objects.equals(that.objects);
    }

    @Override
    public int hashCode() {
        return objects.hashCode();
    }

    @Override
    public String toString() {
        return "ObjectStoreManifest{objects=" + objects + '}';
    }
}
