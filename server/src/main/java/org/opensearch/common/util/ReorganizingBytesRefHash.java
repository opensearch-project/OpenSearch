/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import net.openhft.hashing.LongHashFunction;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.core.common.util.ByteArray;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Specialized hash table implementation that maps a {@link BytesRef} key to a long ordinal.
 *
 * <p>
 * It organizes itself by moving keys around dynamically in order to reduce the
 * longest probe sequence length (PSL), which makes lookups faster as keys are likely to
 * be found in the same CPU cache line. It also uses fingerprints to short-circuit expensive
 * equality checks for keys that collide onto the same hash table slot.
 *
 * <p>
 * This class is not thread-safe.
 *
 * @opensearch.internal
 */
public class ReorganizingBytesRefHash implements Releasable {
    private static final LongHashFunction XX3 = AccessController.doPrivileged(
        (PrivilegedAction<LongHashFunction>) () -> LongHashFunction.xx3(System.nanoTime())
    );

    private static final long MAX_CAPACITY = 1L << 32;
    private static final long DEFAULT_INITIAL_CAPACITY = 32;
    private static final float DEFAULT_LOAD_FACTOR = 0.6f;
    private static final Hasher DEFAULT_HASHER = key -> XX3.hashBytes(key.bytes, key.offset, key.length);

    private static final long MASK_ORDINAL = 0x00000000FFFFFFFFL;  // extract ordinal
    private static final long MASK_FINGERPRINT = 0x0000FFFF00000000L;  // extract fingerprint
    private static final long MASK_PSL = 0x7FFF000000000000L;  // extract PSL
    private static final long INCR_PSL = 0x0001000000000000L;  // increment PSL by one

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
     * |=========|===============|================|================================|
     * | Discard | PSL           | Fingerprint    | Ordinal                        |
     * |    -    |---------------|----------------|--------------------------------|
     * | 1 bit   | 15 bits       | 16 bits        | 32 bits                        |
     * |=========|===============|================|================================|
     * </pre>
     *
     * <p>
     * This allows us to encode and manipulate additional information in the hash table
     * itself without having to look elsewhere in the memory, which is much slower.
     *
     * <p>
     * Terminology: <code>table[index] = value = (discard | psl | fingerprint | ordinal)</code>
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

    public ReorganizingBytesRefHash(final BigArrays bigArrays) {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_HASHER, bigArrays);
    }

    public ReorganizingBytesRefHash(final long initialCapacity, final float loadFactor, final Hasher hasher, final BigArrays bigArrays) {
        assert initialCapacity > 0 : "initial capacity must be greater than 0";
        assert loadFactor > 0 && loadFactor < 1 : "load factor must be between 0 and 1";

        this.loadFactor = loadFactor;
        this.hasher = hasher;
        this.bigArrays = bigArrays;

        capacity = Math.max(1, Long.highestOneBit((long) (initialCapacity / loadFactor)) << 1);
        mask = capacity - 1;
        size = 0;
        grow = (long) (capacity * loadFactor);

        table = bigArrays.newLongArray(capacity, false);
        table.fill(0, capacity, -1);
        offsets = bigArrays.newLongArray(initialCapacity + 1, false);
        offsets.set(0, 0);
        keys = bigArrays.newByteArray(initialCapacity * 3, false);
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
                final long val = (fingerprint | size);
                if (size >= grow) {
                    growAndInsert(hash, val);
                } else {
                    insert(hash, val);
                }
                return append(key);
            } else if (((value & MASK_FINGERPRINT) == fingerprint) && key.bytesEquals(get(ordinal = (value & MASK_ORDINAL), scratch))) {
                return -(1 + ordinal);
            }
        }
    }

    /**
     * Returns the ordinal associated with the given key, or -1 if the key doesn't exist.
     *
     * <p>
     * Using the 64-bit hash value, up to 32 least significant bits (LSB) are used to identify the
     * home slot in the hash table, and an additional 16 bits are used to identify the fingerprint.
     * The fingerprint further increases the entropy and reduces the number of false lookups in the
     * keys' table during equality checks, which is expensive.
     *
     * <p>
     * Total entropy bits = 16 + log2(capacity)
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
    private long append(final BytesRef key) {
        final long start = offsets.get(size);
        final long end = start + key.length;
        offsets = bigArrays.grow(offsets, size + 2);
        offsets.set(size + 1, end);
        keys = bigArrays.grow(keys, end);
        keys.set(start, key.bytes, key.offset, key.length);
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
            final long h = hasher.hash(get(ordinal, scratch));
            insert(h, (h & MASK_FINGERPRINT) | ordinal);
        }
    }

    /**
     * Inserts the hash table value for a missing key.
     */
    private void insert(final long hash, final long value) {
        for (long idx = hash & mask, current = value, existing;; idx = (idx + 1) & mask) {
            if ((existing = table.get(idx)) == -1) {
                table.set(idx, current);
                return;
            } else if ((existing & MASK_PSL) < (current & MASK_PSL)) {
                current = table.set(idx, current);
            }
            current += INCR_PSL;
        }
    }

    @Override
    public void close() {
        Releasables.close(table, offsets, keys);
    }

    /**
     * Returns the underlying hash table.
     * Visible for unit-tests.
     */
    LongArray getTable() {
        return table;
    }

    /**
     * Hasher calculates the hash of a {@link BytesRef} key.
     */
    @FunctionalInterface
    public interface Hasher {
        long hash(BytesRef key);
    }
}
