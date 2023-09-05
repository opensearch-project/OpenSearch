/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.lease.Releasable;

/**
 * Specialized hash table implementation that maps a (primitive) long to long.
 *
 * <p>
 * It organizes itself by moving keys around dynamically in order to reduce the
 * longest probe sequence length (PSL), which makes lookups faster as keys are likely to
 * be found in the same CPU cache line. It also optimizes lookups for recently added keys,
 * making it useful for aggregations where keys are correlated across consecutive hits.
 *
 * <p>
 * This class is not thread-safe.
 *
 * @opensearch.internal
 */
public class ReorganizingLongHash implements Releasable {
    private static final long MAX_CAPACITY = 1L << 32;
    private static final long DEFAULT_INITIAL_CAPACITY = 32;
    private static final float DEFAULT_LOAD_FACTOR = 0.6f;

    /**
     * Maximum load factor after which the capacity is doubled.
     */
    private final float loadFactor;

    /**
     * Utility class to allocate recyclable arrays.
     */
    private final BigArrays bigArrays;

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
     * Underlying array to store the keys.
     *
     * <p>
     * Terminology: <code>keys[ordinal] = key</code>
     */
    private LongArray keys;

    /**
     * Bitmasks to manipulate the hash table values.
     */
    private static final long MASK_ORDINAL = 0x00000000FFFFFFFFL;  // extract ordinal
    private static final long MASK_FINGERPRINT = 0x0000FFFF00000000L;  // extract fingerprint
    private static final long MASK_PSL = 0x7FFF000000000000L;  // extract PSL
    private static final long INCR_PSL = 0x0001000000000000L;  // increment PSL by one

    public ReorganizingLongHash(final BigArrays bigArrays) {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, bigArrays);
    }

    public ReorganizingLongHash(final long initialCapacity, final float loadFactor, final BigArrays bigArrays) {
        assert initialCapacity > 0 : "initial capacity must be greater than 0";
        assert loadFactor > 0 && loadFactor < 1 : "load factor must be between 0 and 1";

        this.bigArrays = bigArrays;
        this.loadFactor = loadFactor;

        capacity = nextPowerOfTwo((long) (initialCapacity / loadFactor));
        mask = capacity - 1;
        grow = (long) (capacity * loadFactor);
        size = 0;

        table = bigArrays.newLongArray(capacity, false);
        table.fill(0, capacity, -1);  // -1 represents an empty slot
        keys = bigArrays.newLongArray(initialCapacity, false);
    }

    /**
     * Adds the given key to the hash table and returns its ordinal.
     * If the key exists already, it returns (-1 - ordinal).
     */
    public long add(final long key) {
        final long ordinal = find(key);
        if (ordinal != -1) {
            return -1 - ordinal;
        }

        if (size >= grow) {
            grow();
        }

        return insert(key);
    }

    /**
     * Returns the key associated with the given ordinal.
     * The result is undefined for an unused ordinal.
     */
    public long get(final long ordinal) {
        return keys.get(ordinal);
    }

    /**
     * Returns the ordinal associated with the given key, or -1 if the key doesn't exist.
     *
     * <p>
     * Using the 64-bit hash value, up to 32 least significant bits (LSB) are used to identify the
     * home slot in the hash table, and an additional 16 bits are used to identify the fingerprint.
     * The fingerprint further increases the entropy and reduces the number of false lookups in the
     * keys' table during equality checks, which is expensive due to an uncorrelated memory lookup.
     *
     * <p>
     * Total entropy bits = 16 + log2(capacity)
     *
     * <p>
     * Linear probing starts from the home slot, until a match or an empty slot is found.
     * Values are first checked using their fingerprint (to reduce false positives), then verified
     * in the keys' table using an equality check.
     */
    public long find(final long key) {
        final long hash = hash(key);
        final long fingerprint = hash & MASK_FINGERPRINT;

        for (long idx = hash & mask, value, ordinal;; idx = (idx + 1) & mask) {
            if ((value = table.get(idx)) == -1) {
                return -1;
            } else if (((value & MASK_FINGERPRINT) == fingerprint) && (keys.get((ordinal = (value & MASK_ORDINAL))) == key)) {
                return ordinal;
            }
        }
    }

    /**
     * Returns the number of mappings in this hash table.
     */
    public long size() {
        return size;
    }

    /**
     * Inserts the given key in the hash table and returns its ordinal.
     *
     * <p>
     * Inspired by Robin Hood Hashing (RHH): if the PSL for the existing value is less than the PSL
     * for the value being inserted, swap the two values and keep going. Values that were inserted
     * early and thus "lucked out" on their PSLs will gradually be moved away from their preferred
     * slot as new values come in that could make better use of that place in the table. It evens out
     * the PSLs across the board and reduces the longest PSL dramatically.
     *
     * <p>
     * A lower variance is better because, with modern CPU architectures, a PSL of 1 isn't much
     * faster than a PSL of 3, because the main cost is fetching the cache line. The ideal hash
     * table layout is the one where all values have equal PSLs, and that PSL fits within one cache line.
     *
     * <p>
     * The expected longest PSL for a full table: <code>log(N)</code>
     *
     * <p>
     * Our implementation has a slight variation on top of it: by loosening the guarantees provided
     * by RHH, we can improve the performance on correlated lookups (such as aggregating on repeated
     * timestamps) by moving the "recent" keys closer to their home slot, and eventually converging
     * to the ideal hash table layout defined by RHH.
     */
    private long insert(final long key) {
        final long hash = hash(key);
        final long fingerprint = hash & MASK_FINGERPRINT;

        // The ideal home slot for the given key.
        long idx = hash & mask;

        // The value yet to find an empty slot (candidate).
        long value = fingerprint | size;

        // The existing value at idx.
        long existingValue;

        // Always set the newly inserted key at its ideal home slot, even if it doesn't conform
        // to the RHH scheme (yet). This will ensure subsequent correlated lookups are fast due
        // to no additional probing. When another insertion causes this value to be displaced, it
        // will eventually be placed at an appropriate location defined by the RHH scheme.
        if ((value = table.set(idx, value)) == -1) {
            // The ideal home slot was already empty; append the key and return early.
            return append(key);
        }

        // Find an alternative slot for the displaced value such that the longest PSL is minimized.
        do {
            idx = (idx + 1) & mask;
            value += INCR_PSL;

            if ((existingValue = table.get(idx)) == -1) {
                // Empty slot; insert the candidate value here.
                table.set(idx, value);
                return append(key);
            } else if ((existingValue & MASK_PSL) <= (value & MASK_PSL)) {
                // Existing value is "richer" than the candidate value at this index;
                // swap and find an alternative slot for the displaced value.
                // In the case of a tie, the candidate value (i.e. the recent value) is chosen as
                // the winner and kept closer to its ideal home slot in order to speed up
                // correlated lookups.
                value = table.set(idx, value);
            }
        } while (true);
    }

    /**
     * Appends the key in the keys' table.
     */
    private long append(final long key) {
        keys = bigArrays.grow(keys, size + 1);
        keys.set(size, key);
        return size++;
    }

    /**
     * Returns the hash for the given key.
     * Visible for unit-tests.
     */
    long hash(final long key) {
        return BitMixer.mix64(key);
    }

    /**
     * Returns the underlying hash table.
     * Visible for unit-tests.
     */
    LongArray getTable() {
        return table;
    }

    /**
     * Grows the hash table by doubling its capacity and reinserting the keys.
     */
    private void grow() {
        // Ensure that the hash table doesn't grow too large.
        // This implicitly also ensures that the ordinals are no larger than 2^32, thus,
        // preventing them from polluting other bits (PSL/fingerprint) in the hash table values.
        assert capacity < MAX_CAPACITY : "hash table already at the max capacity";

        final long oldSize = size;
        capacity <<= 1;
        mask = capacity - 1;
        size = 0;
        grow = (long) (capacity * loadFactor);
        table = bigArrays.resize(table, capacity);
        table.fill(0, capacity, -1);

        for (long ordinal = 0; ordinal < oldSize; ordinal++) {
            insert(keys.get(ordinal));
        }
    }

    @Override
    public void close() {
        table.close();
        keys.close();
    }

    private static long nextPowerOfTwo(final long value) {
        return Math.max(1, Long.highestOneBit(value - 1) << 1);
    }
}
