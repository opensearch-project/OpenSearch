/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import com.carrotsearch.hppc.BitMixer;
import org.opensearch.common.lease.Releasable;

import java.util.Arrays;

/**
 * Specialized hash table implementation that maps (primitive) long to long.
 *
 * It organizes itself by moving keys around dynamically in order to reduce the longest
 * probe sequence length (PSL). It also optimizes lookups for recently accessed keys, which
 * makes it very useful for aggregations where keys are correlated across consecutive hits.
 *
 * This class is not thread-safe.
 *
 * @opensearch.internal
 */
public class LongRHHash implements Releasable {
    private static final long DEFAULT_CAPACITY = 1;
    private static final float DEFAULT_LOAD_FACTOR = 0.6f;
    private static final int DEFAULT_HINTS = 256;

    /**
     * Utility class to allocate big arrays.
     */
    private final BigArrays bigArrays;

    /**
     * Maximum load factor after which the capacity is doubled.
     */
    private final float loadFactor;

    /**
     * Current capacity of the hash table.
     * This must always be a power of two since we use a bit-mask to calculate the slot.
     */
    private long capacity;

    /**
     * Bit-mask used to calculate the slot (i.e. the index) after hashing a key.
     */
    private long mask;

    /**
     * Current size of the hash table.
     */
    private long size;

    /**
     * Threshold at which the hash table needs to be resized.
     */
    private long grow;

    /**
     * Lookup table for ordinals.
     */
    private LongArray ords;

    /**
     * Lookup table for keys.
     */
    private LongArray keys;

    /**
     * Lookup table for "recent" key hints.
     */
    private final long[] hints;

    /**
     * Bit-mask used to calculate the slot in the hints' table.
     */
    private final int hintMask;

    public LongRHHash(BigArrays bigArrays) {
        this(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_HINTS, bigArrays);
    }

    public LongRHHash(long capacity, float loadFactor, int numHints, BigArrays bigArrays) {
        capacity = (long) (capacity / loadFactor);
        capacity = Math.max(1, Long.highestOneBit(capacity - 1) << 1); // next power of two

        this.loadFactor = loadFactor;
        this.bigArrays = bigArrays;
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.size = 0;
        this.grow = (long) (capacity * loadFactor);

        this.ords = bigArrays.newLongArray(capacity, false);
        this.ords.fill(0, capacity, -1);
        this.keys = bigArrays.newLongArray(capacity, false);

        numHints = Math.max(1, Integer.highestOneBit(numHints - 1) << 1);
        this.hintMask = numHints - 1;
        this.hints = new long[numHints];
        Arrays.fill(this.hints, -1);
    }

    /**
     * Adds the key to the hash table and returns its ordinal (i.e. the index) starting from zero.
     * If the key exists already, it returns (-1-ordinal).
     */
    public long add(final long key) {
        final long ordinal = find(key);
        if (ordinal != -1) {
            return -(1 + ordinal);
        }

        if (size >= grow) {
            grow();
        }
        return insert(key, size);
    }

    /**
     * Associates the given key with the given ordinal.
     *
     * This is inspired by Robin Hood hashing, but not implemented wholly.
     * Keys are moved around dynamically in order to reduce the longest probe sequence length (PSL),
     * which makes lookups faster as keys are likely to be found in the same CPU cache line.
     */
    private long insert(final long key, final long ordinal) {
        long idx = slot(key);   // The ideal "home" location for the given key in the ordinals' table.
        long ord = ordinal;     // The ordinal yet to find an empty slot in the ordinals' table (candidate).
        long psl = 0;           // The displacement of "ord" from its home location.

        long curOrd;            // The current ordinal stored at "idx".
        long curPsl;            // The current ordinal's PSL.

        do {
            if ((curOrd = ords.get(idx)) == -1) {
                // Empty slot; insert the candidate ordinal here.
                ords.set(idx, ord);
                keys = bigArrays.grow(keys, size + 1);
                keys.set(ordinal, key);
                return size++;
            } else if ((curPsl = psl(keys.get(curOrd), idx)) < psl) {
                // Current key is "richer" than the candidate key at this slot; swap and find a new home for it.
                ord = ords.set(idx, ord);
                psl = curPsl;
            }
            idx = (idx + 1) & mask;
            psl++;
        } while (true);
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
     * It is inspired by Cuckoo hashing but with a twist:
     * A fast hash function is used to look up "recent" keys, then falling back to a good hash function with
     * linear probing. Since two independent hash functions are used, it is unlikely that both collide.
     *
     * This is very effective for most bucket aggregations where keys are likely to be correlated across consecutive
     * hits (e.g. timestamps may belong to the same "hour" in the case of date histogram aggregations).
     */
    public long find(final long key) {
        int hintIdx = hintSlot(key);
        long hintOrd = hints[hintIdx];
        if (hintOrd != -1 && keys.get(hintOrd) == key) {
            return hintOrd;
        }

        for (long idx = slot(key);; idx = (idx + 1) & mask) {
            final long ord = ords.get(idx);
            if (ord == -1 || keys.get(ord) == key) {
                hints[hintIdx] = ord;
                return ord;
            }
        }
    }

    /**
     * Returns the current size of the hash table.
     */
    public long size() {
        return size;
    }

    /**
     * Returns the "home" location for the given key in the ordinals' table.
     */
    private long slot(final long key) {
        return BitMixer.mix64(key) & mask;
    }

    /**
     * Returns the "home" location for the given key in the hints' table.
     */
    private int hintSlot(long key) {
        return BitMixer.mixPhi(key) & hintMask;
    }

    /**
     * Returns the probe sequence length (PSL) for the given key at the given index.
     */
    private long psl(final long key, final long idx) {
        return (capacity + idx - slot(key)) & mask;
    }

    /**
     * Grows the size of the hash table by doubling its capacity.
     */
    private void grow() {
        final long oldSize = size;

        capacity <<= 1;
        mask = capacity - 1;
        size = 0;
        grow = (long) (capacity * loadFactor);

        ords = bigArrays.resize(ords, capacity);
        ords.fill(0, capacity, -1);

        for (long ordinal = 0; ordinal < oldSize; ordinal++) {
            insert(keys.get(ordinal), ordinal);
        }
    }

    @Override
    public void close() {
        ords.close();
        keys.close();
    }
}
