/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.policy;

/**
 * A compact, self-aging Count-Min Sketch for approximate access-frequency estimation (the TinyLFU building block).
 *
 * <p><b>NOT THREAD-SAFE.</b> Callers must confine it to a single thread or provide external synchronization (see the
 * Thread-safety note below for why unsynchronized use is nonetheless only lossy, not corrupting).
 *
 * <p>Layout: a {@code long[]} table where each {@code long} packs sixteen 4-bit saturating counters (64 / 4 = 16).
 * Each key maps to four counters via four independent hashes, and its estimated frequency is the minimum of the four
 * (the "min" of Count-Min cancels most of the over-counting caused by hash collisions). Counters saturate at 15, which
 * is ample for an admission threshold. Once the number of increments reaches a sampling threshold, every counter is
 * halved in a single shift+mask pass so estimates track <em>recent</em> frequency in fixed memory.
 *
 * <p>Memory is ~8 bytes per tracked entry (one {@code long} per 16 counters, table sized to the requested capacity),
 * independent of how many distinct keys are actually seen.
 *
 * <p>Design (4-bit packed counters, min-of-4, periodic halving) follows the TinyLFU admission policy and is inspired by
 * Caffeine's {@code FrequencySketch}.
 *
 * <p><b>Thread-safety:</b> not synchronized. A concurrent race can at most lose an increment (a slight under-count),
 * never corrupt memory, which is fine for a frequency estimator.
 */
public final class FrequencySketch<T> {

    // Clears the top bit of every 4-bit nibble, used to halve all counters at once (see reset()).
    private static final long RESET_MASK = 0x7777777777777777L;

    private final long[] table;
    private final int tableMask;
    private final int sampleSize;
    private int size;

    // Distinct multipliers to derive four independent counter locations from one base hash.
    private static final int[] SEED = { 0x7f4a7c15, 0x9e3779b1, 0x85ebca77, 0xc2b2ae3d };

    /**
     * @param expectedInsertions the number of entries the sketch should be sized for (typically the cache capacity in
     *                            entries). The table is rounded up to a power of two, and a small floor is applied so
     *                            tiny caches still get a usable sketch.
     */
    public FrequencySketch(long expectedInsertions) {
        int len = tableSizeFor(Math.max(expectedInsertions, 8));
        this.table = new long[len];
        this.tableMask = len - 1;
        // Reset after ~10x capacity increments (Caffeine's heuristic), bounded to avoid overflow.
        long sample = 10L * len;
        this.sampleSize = (sample > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) sample;
    }

    /** Returns the estimated access frequency of {@code e} (0..15). */
    public int frequency(T e) {
        int hash = spread(e.hashCode());
        int frequency = Integer.MAX_VALUE;
        for (int i = 0; i < 4; i++) {
            int offset = counterOffset(hash, i);
            int count = (int) ((table[indexOf(hash, i)] >>> offset) & 0xfL);
            frequency = Math.min(frequency, count);
        }
        return frequency;
    }

    /** Records one access of {@code e}. */
    public void increment(T e) {
        int hash = spread(e.hashCode());
        boolean incremented = false;
        for (int i = 0; i < 4; i++) {
            incremented |= incrementAt(indexOf(hash, i), counterOffset(hash, i));
        }
        if (incremented && (++size >= sampleSize)) {
            reset();
        }
    }

    // Increments the 4-bit counter at (index, offset) unless it is already saturated at 15. Returns true if changed.
    private boolean incrementAt(int index, int offset) {
        long mask = 0xfL << offset;
        if ((table[index] & mask) != mask) {
            table[index] += (1L << offset);
            return true;
        }
        return false;
    }

    // Halves every counter (aging). Package-private for testing, normally triggered automatically by increment().
    void reset() {
        for (int i = 0; i < table.length; i++) {
            table[i] = (table[i] >>> 1) & RESET_MASK;
        }
        size = size >>> 1;
    }

    // The table word for counter i.
    private int indexOf(int hash, int i) {
        int h = hash * SEED[i];
        h ^= h >>> 15;
        return h & tableMask;
    }

    // The bit offset (0,4,8,..,60) of the nibble for counter i within its word.
    private int counterOffset(int hash, int i) {
        int h = hash * SEED[i];
        return ((h >>> 24) & 0xf) << 2;
    }

    // Supplemental hash to defend against poor hashCode distributions (murmur3-style finalizer).
    private static int spread(int x) {
        x ^= x >>> 17;
        x *= 0xed5ad4bb;
        x ^= x >>> 11;
        x *= 0xac4c1b51;
        x ^= x >>> 15;
        return x;
    }

    private static int tableSizeFor(long n) {
        int cap = Integer.highestOneBit((int) Math.min(n - 1, Integer.MAX_VALUE >> 1));
        return Math.max(1, cap << 1);
    }

    // Package-private for testing.
    int sampleSize() {
        return sampleSize;
    }
}
