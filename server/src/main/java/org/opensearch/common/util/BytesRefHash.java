/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Numbers;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.hash.T1ha1;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.core.common.util.ByteArray;

/**
 * Specialized hash table implementation that maps a {@link BytesRef} key to a long ordinal.
 *
 * <p>
 * It uses a compact byte-packing strategy to encode the ordinal and fingerprint information
 * in the hash table value. It makes lookups faster by short-circuiting expensive equality checks
 * for keys that collide onto the same hash table slot.
 *
 * <p>
 * This class is not thread-safe.
 *
 * @opensearch.internal
 */
@InternalApi
public final class BytesRefHash implements Releasable {
    private static final long MAX_CAPACITY = 1L << 32;
    private static final long DEFAULT_INITIAL_CAPACITY = 32;
    private static final float DEFAULT_LOAD_FACTOR = 0.6f;
    private static final Hasher DEFAULT_HASHER = key -> T1ha1.hash(key.bytes, key.offset, key.length);

    private static final long MASK_ORDINAL = 0x00000000FFFFFFFFL;  // extract ordinal
    private static final long MASK_FINGERPRINT = 0xFFFFFFFF00000000L;  // extract fingerprint

    /**
     * Maximum load factor after which the capacity is doubled.
     */
    private final float loadFactor;

    /**
     * Calculates the hash of a {@link BytesRef} key.
     */
    private final Hasher hasher;

    /**
     * Utility class to allocate recyclable arrays.
     */
    private final BigArrays bigArrays;

    /**
     * Reusable BytesRef to read keys.
     */
    private final BytesRef scratch = new BytesRef();

    /**
     * Current capacity of the hash table. This must be a power of two so that the hash table slot
     * can be identified quickly using bitmasks, thus avoiding expensive modulo or integer division.
     */
    private long capacity;

    /**
     * Bitmask to identify the hash table slot from a key's hash.
     */
    private long mask;

    /**
     * Size threshold after which the hash table needs to be doubled in capacity.
     */
    private long grow;

    /**
     * Current size of the hash table.
     */
    private long size;

    /**
     * Underlying array to store the hash table values.
     *
     * <p>
     * Each hash table value (64-bit) uses the following byte packing strategy:
     * <pre>
     * |================================|================================|
     * | Fingerprint                    | Ordinal                        |
     * |--------------------------------|--------------------------------|
     * | 32 bits                        | 32 bits                        |
     * |================================|================================|
     * </pre>
     *
     * <p>
     * This allows us to encode and manipulate additional information in the hash table
     * itself without having to look elsewhere in the memory, which is much slower.
     *
     * <p>
     * Terminology: <code>table[index] = value = (fingerprint | ordinal)</code>
     */
    private LongArray table;

    /**
     * Underlying array to store the starting offsets of keys.
     *
     * <p>
     * Terminology:
     * <pre>
     *   offsets[ordinal] = starting offset (inclusive)
     *   offsets[ordinal + 1] = ending offset (exclusive)
     * </pre>
     */
    private LongArray offsets;

    /**
     * Underlying byte array to store the keys.
     *
     * <p>
     * Terminology: <code>keys[start...end] = key</code>
     */
    private ByteArray keys;

    /**
     * Pre-computed hashes of the stored keys.
     * It is used to speed up reinserts when doubling the capacity.
     */
    private LongArray hashes;

    public BytesRefHash(final BigArrays bigArrays) {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_HASHER, bigArrays);
    }

    public BytesRefHash(final long initialCapacity, final BigArrays bigArrays) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_HASHER, bigArrays);
    }

    public BytesRefHash(final long initialCapacity, final float loadFactor, final BigArrays bigArrays) {
        this(initialCapacity, loadFactor, DEFAULT_HASHER, bigArrays);
    }

    public BytesRefHash(final long initialCapacity, final float loadFactor, final Hasher hasher, final BigArrays bigArrays) {
        assert initialCapacity > 0 : "initial capacity must be greater than 0";
        assert loadFactor > 0 && loadFactor < 1 : "load factor must be between 0 and 1";

        this.loadFactor = loadFactor;
        this.hasher = hasher;
        this.bigArrays = bigArrays;

        capacity = Numbers.nextPowerOfTwo((long) (initialCapacity / loadFactor));
        assert capacity <= MAX_CAPACITY : "required capacity too large";
        mask = capacity - 1;
        size = 0;
        grow = (long) (capacity * loadFactor);

        table = bigArrays.newLongArray(capacity, false);
        table.fill(0, capacity, -1);
        offsets = bigArrays.newLongArray(initialCapacity + 1, false);
        offsets.set(0, 0);
        keys = bigArrays.newByteArray(initialCapacity * 3, false);
        hashes = bigArrays.newLongArray(initialCapacity, false);
    }

    /**
     * Adds the given key to the hash table and returns its ordinal.
     * If the key exists already, it returns (-1 - ordinal).
     */
    public long add(final BytesRef key) {
        final long hash = hasher.hash(key);
        final long fingerprint = hash & MASK_FINGERPRINT;

        for (long idx = hash & mask, value, ordinal;; idx = (idx + 1) & mask) {
            if ((value = table.get(idx)) == -1) {
                final long val = fingerprint | size;
                if (size >= grow) {
                    growAndInsert(hash, val);
                } else {
                    table.set(idx, val);
                }
                return append(key, hash);
            } else if (((value & MASK_FINGERPRINT) == fingerprint) && key.bytesEquals(get(ordinal = (value & MASK_ORDINAL), scratch))) {
                return -1 - ordinal;
            }
        }
    }

    /**
     * Returns the ordinal associated with the given key, or -1 if the key doesn't exist.
     *
     * <p>
     * Using the 64-bit hash value, up to 32 least significant bits (LSB) are used to identify the
     * home slot in the hash table, and an additional 32 bits are used to identify the fingerprint.
     * The fingerprint further increases the entropy and reduces the number of false lookups in the
     * keys' table during equality checks, which is expensive.
     *
     * <p>
     * Total entropy bits = 32 + log2(capacity)
     *
     * <p>
     * Linear probing starts from the home slot, until a match or an empty slot is found.
     * Values are first checked using their fingerprint (to reduce false positives), then verified
     * in the keys' table using an equality check.
     */
    public long find(final BytesRef key) {
        final long hash = hasher.hash(key);
        final long fingerprint = hash & MASK_FINGERPRINT;

        for (long idx = hash & mask, value, ordinal;; idx = (idx + 1) & mask) {
            if ((value = table.get(idx)) == -1) {
                return -1;
            } else if (((value & MASK_FINGERPRINT) == fingerprint) && key.bytesEquals(get(ordinal = (value & MASK_ORDINAL), scratch))) {
                return ordinal;
            }
        }
    }

    /**
     * Returns the key associated with the given ordinal.
     * The result is undefined for an unused ordinal.
     *
     * <p>
     * Beware that the content of the {@link BytesRef} may become invalid as soon as {@link #close()} is called
     */
    public BytesRef get(final long ordinal, final BytesRef dest) {
        final long start = offsets.get(ordinal);
        final int length = (int) (offsets.get(ordinal + 1) - start);
        keys.get(start, length, dest);
        return dest;
    }

    /**
     * Returns the number of mappings in this hash table.
     */
    public long size() {
        return size;
    }

    /**
     * Appends the key in the keys' and offsets' tables.
     */
    private long append(final BytesRef key, final long hash) {
        final long start = offsets.get(size);
        final long end = start + key.length;
        offsets = bigArrays.grow(offsets, size + 2);
        offsets.set(size + 1, end);
        keys = bigArrays.grow(keys, end);
        keys.set(start, key.bytes, key.offset, key.length);
        hashes = bigArrays.grow(hashes, size + 1);
        hashes.set(size, hash);
        return size++;
    }

    /**
     * Grows the hash table by doubling its capacity, inserting the provided value,
     * and reinserting the previous values at their updated slots.
     */
    private void growAndInsert(final long hash, final long value) {
        // Ensure that the hash table doesn't grow too large.
        // This implicitly also ensures that the ordinals are no larger than 2^32, thus,
        // preventing them from polluting the fingerprint bits in the hash table values.
        assert capacity < MAX_CAPACITY : "hash table already at the max capacity";

        capacity <<= 1;
        mask = capacity - 1;
        grow = (long) (capacity * loadFactor);
        table = bigArrays.grow(table, capacity);
        table.fill(0, capacity, -1);
        table.set(hash & mask, value);

        for (long ordinal = 0; ordinal < size; ordinal++) {
            reinsert(ordinal, hashes.get(ordinal));
        }
    }

    /**
     * Reinserts the hash table value for an existing key stored at the given ordinal.
     */
    private void reinsert(final long ordinal, final long hash) {
        for (long idx = hash & mask;; idx = (idx + 1) & mask) {
            if (table.get(idx) == -1) {
                table.set(idx, (hash & MASK_FINGERPRINT) | ordinal);
                return;
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(table, offsets, keys, hashes);
    }

    /**
     * Hasher calculates the hash of a {@link BytesRef} key.
     */
    @FunctionalInterface
    public interface Hasher {
        long hash(BytesRef key);
    }
}
