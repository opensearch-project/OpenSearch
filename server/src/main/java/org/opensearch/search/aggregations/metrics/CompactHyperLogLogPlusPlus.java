/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.IntArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.core.common.util.ByteArray;

import java.util.function.LongConsumer;

/**
 * HyperLogLog++ sketches for many buckets where each bucket only pays for the values it holds.
 * <p>
 * {@link HyperLogLogPlusPlus} reserves {@code 2^precision} bytes for every bucket up to the highest one collected, and keeps
 * the linear counting hash set of a bucket inside those bytes. Under a parent aggregation with many buckets that is
 * {@code buckets * 2^precision} bytes, 16KB per bucket at the default precision, even when most buckets hold a handful of
 * values. Here a bucket in linear counting mode keeps a single hash inline, then its hashes in its own hash table that
 * starts with {@value #MIN_TABLE_SIZE} slots and doubles at 75% load, and it only gets {@code 2^precision} bytes of registers
 * once it holds more hashes than linear counting allows.
 * <p>
 * The encoding of the hashes, the linear counting threshold and the switch to HyperLogLog are the same as in
 * {@link HyperLogLogPlusPlus}, so for the same values both produce the same sketches and the same cardinalities. A bucket
 * never uses more than about 24 bytes more than it would in {@link HyperLogLogPlusPlus}.
 * <p>
 * The hash tables are plain {@code int[]} arrays whose estimated size is charged to {@code breaker}, in batches of
 * {@value #BREAKER_FLUSH_BYTES} bytes. The registers and the per-bucket bookkeeping come from {@code bigArrays}. The
 * HyperLogLog estimates are cached until a register changes, because ordering buckets by cardinality asks for the same
 * buckets many times.
 *
 * @opensearch.internal
 */
final class CompactHyperLogLogPlusPlus extends AbstractHyperLogLogPlusPlus {

    static final int MIN_TABLE_SIZE = 4;
    static final long BREAKER_FLUSH_BYTES = 64 * 1024;
    // Fibonacci hashing spreads the encoded hashes, whose lowest bit is a flag, over small tables
    private static final int TABLE_HASH_MULTIPLIER = 0x9E3779B9;

    private final BigArrays bigArrays;
    private final LongConsumer breaker;
    /** Maximum number of hashes a bucket keeps in linear counting mode, the same as {@link HyperLogLogPlusPlus}. */
    private final int threshold;
    /** Size of the hash table that holds {@link #threshold} hashes at 75% load: {@code 2^precision / 4} ints. */
    private final int maxTableSize;
    private final LinearCounting lc;
    private final HyperLogLog hll;

    /**
     * Hash table of each bucket in linear counting mode with at least two hashes, {@code null} for the other buckets. Only
     * grown to the highest such bucket.
     */
    private ObjectArray<int[]> tables;
    /** Number of hashes of a bucket in linear counting mode, or {@code -(slot + 1)} for a bucket whose registers are in {@code slot}. */
    private IntArray states;
    /**
     * The only hash of a bucket holding one, kept inline as most buckets under a parent with many buckets hold a single value
     * and a table would cost several times more. Meaningless for buckets with any other number of hashes.
     */
    private IntArray firstHashes;
    /** {@code 2^precision} registers for each bucket in HyperLogLog mode, indexed by slot. */
    private ByteArray registers;
    /** Cached HyperLogLog estimate of each slot. */
    private LongArray cardinalities;
    /** {@link #registerChanges} + 1 when the estimate of a slot was cached, which makes it stale once a register is updated. */
    private LongArray cardinalityVersions;
    /** Number of register updates, counted instead of invalidating the cache of a slot to keep collection cheap. */
    private long registerChanges;
    private long slots;
    private long tableBytes;
    private long breakerBytes;

    CompactHyperLogLogPlusPlus(int precision, BigArrays bigArrays, LongConsumer breaker) {
        super(precision);
        this.bigArrays = bigArrays;
        this.breaker = breaker;
        this.maxTableSize = (1 << precision) / 4;
        this.threshold = (int) (maxTableSize * 0.75f);
        this.lc = new LinearCounting(precision);
        this.hll = new HyperLogLog(precision);
        boolean success = false;
        try {
            tables = bigArrays.newObjectArray(1);
            states = bigArrays.newIntArray(1, true);
            firstHashes = bigArrays.newIntArray(1, false);
            registers = bigArrays.newByteArray(0, true);
            cardinalities = bigArrays.newLongArray(0, false);
            cardinalityVersions = bigArrays.newLongArray(0, true);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    /**
     * Estimated heap used by the hash table of one bucket, the rest is accounted for by {@link BigArrays}.
     */
    static long tableBytes(int size) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * size);
    }

    /**
     * Number of buckets that switched to HyperLogLog.
     */
    long hyperLogLogBuckets() {
        return slots;
    }

    /**
     * Estimated heap used by the linear counting hash tables.
     */
    long linearCountingBytes() {
        return tableBytes;
    }

    @Override
    public long maxOrd() {
        return states.size();
    }

    private int state(long bucketOrd) {
        return bucketOrd < states.size() ? states.get(bucketOrd) : 0;
    }

    private static long slot(int state) {
        assert state < 0;
        return -1L - state;
    }

    @Override
    protected boolean getAlgorithm(long bucketOrd) {
        return state(bucketOrd) < 0 ? HYPERLOGLOG : LINEAR_COUNTING;
    }

    @Override
    public long cardinality(long bucketOrd) {
        final int state = state(bucketOrd);
        if (state >= 0) {
            return lc.cardinality(bucketOrd);
        }
        final long slot = slot(state);
        if (cardinalityVersions.get(slot) == registerChanges + 1) {
            return cardinalities.get(slot);
        }
        final long cardinality = hll.cardinality(slot);
        cardinalities.set(slot, cardinality);
        cardinalityVersions.set(slot, registerChanges + 1);
        return cardinality;
    }

    @Override
    protected AbstractLinearCounting.HashesIterator getLinearCounting(long bucketOrd) {
        return lc.values(bucketOrd);
    }

    @Override
    protected AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd) {
        return hll.getRunLens(slot(state(bucketOrd)));
    }

    @Override
    public void collect(long bucketOrd, long hash) {
        final int state = state(bucketOrd);
        if (state < 0) {
            // Same register and run length as AbstractHyperLogLog#collect, inlined as this is the hot path of big buckets
            final int register = (int) (hash >>> (64 - p));
            final int runLen = 1 + Math.min(Long.numberOfLeadingZeros(hash << p), 64 - p);
            hll.addRunLen(slot(state), register, runLen);
        } else {
            lc.collect(bucketOrd, hash);
        }
    }

    /**
     * Gives {@code bucketOrd} registers, hashes its linear counting values into them, then adds {@code encoded}. Its values
     * are in {@code table}, or inline in {@link #firstHashes} if {@code table} is {@code null}.
     * The registers are allocated before anything changes, so a tripped circuit breaker leaves the bucket as it was.
     */
    private void upgradeToHll(long bucketOrd, int[] table, int encoded) {
        final long slot = slots;
        registers = bigArrays.grow(registers, (slot + 1) << p);
        cardinalities = bigArrays.grow(cardinalities, slot + 1);
        cardinalityVersions = bigArrays.grow(cardinalityVersions, slot + 1);
        registers.fill(slot << p, (slot + 1) << p, (byte) 0);
        cardinalityVersions.set(slot, 0);
        slots++;
        if (table != null) {
            for (int value : table) {
                if (value != 0) {
                    hll.collectEncoded(slot, value);
                }
            }
        } else if (states.get(bucketOrd) == 1) {
            hll.collectEncoded(slot, firstHashes.get(bucketOrd));
        }
        hll.collectEncoded(slot, encoded);
        states.set(bucketOrd, Math.toIntExact(-1L - slot));
        if (table != null) {
            tables.set(bucketOrd, null);
            accountTable(-tableBytes(table.length));
        }
    }

    private void accountTable(long bytes) {
        tableBytes += bytes;
        final long delta = tableBytes - breakerBytes;
        if (delta >= BREAKER_FLUSH_BYTES || delta <= -BREAKER_FLUSH_BYTES) {
            breaker.accept(delta);
            breakerBytes += delta;
        }
    }

    @Override
    public void close() {
        try {
            if (breakerBytes != 0) {
                breaker.accept(-breakerBytes);
            }
        } finally {
            breakerBytes = 0;
            tableBytes = 0;
            Releasables.close(tables, states, firstHashes, registers, cardinalities, cardinalityVersions);
        }
    }

    /**
     * Linear counting over the per-bucket hash tables.
     */
    private class LinearCounting extends AbstractLinearCounting {

        private final LinearCountingIterator iterator = new LinearCountingIterator();

        LinearCounting(int precision) {
            super(precision);
        }

        private int indexOf(int encoded, int tableSize) {
            return (encoded * TABLE_HASH_MULTIPLIER) >>> (Integer.SIZE - Integer.numberOfTrailingZeros(tableSize));
        }

        /**
         * Adds {@code encoded} to the bucket and returns its new number of hashes, or {@code -1} if it was already there.
         * A bucket keeps its first hash inline and gets a table for its second one. A bucket that would go over the
         * threshold switches to HyperLogLog instead.
         */
        @Override
        protected int addEncoded(long bucketOrd, int encoded) {
            assert encoded != 0;
            final int size = bucketOrd < states.size() ? states.get(bucketOrd) : 0;
            assert size >= 0 : "bucket is in HyperLogLog mode";
            if (size == 0) {
                states = bigArrays.grow(states, bucketOrd + 1);
                firstHashes = bigArrays.grow(firstHashes, bucketOrd + 1);
                firstHashes.set(bucketOrd, encoded);
                states.set(bucketOrd, 1);
                return 1;
            }
            int[] table = bucketOrd < tables.size() ? tables.get(bucketOrd) : null;
            // the empty slot where encoded goes if the table keeps its size
            int slot = -1;
            if (table == null) {
                assert size == 1;
                if (firstHashes.get(bucketOrd) == encoded) {
                    return -1;
                }
            } else {
                final int mask = table.length - 1;
                for (int i = indexOf(encoded, table.length);; i = (i + 1) & mask) {
                    final int v = table[i];
                    if (v == encoded) {
                        return -1;
                    }
                    if (v == 0) {
                        slot = i;
                        break;
                    }
                }
            }
            final int newSize = size + 1;
            if (newSize > threshold) {
                upgradeToHll(bucketOrd, table, encoded);
                return newSize;
            }
            if (table == null) {
                table = newTable(bucketOrd, Math.min(MIN_TABLE_SIZE, maxTableSize));
                insert(table, firstHashes.get(bucketOrd));
                insert(table, encoded);
            } else if (newSize > (table.length >>> 2) * 3) {
                assert table.length < maxTableSize;
                table = grow(bucketOrd, table);
                insert(table, encoded);
            } else {
                table[slot] = encoded;
            }
            states.set(bucketOrd, newSize);
            return newSize;
        }

        private int[] newTable(long bucketOrd, int size) {
            tables = bigArrays.grow(tables, bucketOrd + 1);
            final int[] table = new int[size];
            tables.set(bucketOrd, table);
            accountTable(tableBytes(size));
            return table;
        }

        private int[] grow(long bucketOrd, int[] table) {
            final int[] grown = new int[table.length << 1];
            for (int value : table) {
                if (value != 0) {
                    insert(grown, value);
                }
            }
            tables.set(bucketOrd, grown);
            accountTable(tableBytes(grown.length) - tableBytes(table.length));
            return grown;
        }

        private void insert(int[] table, int encoded) {
            final int mask = table.length - 1;
            int i = indexOf(encoded, table.length);
            while (table[i] != 0) {
                i = (i + 1) & mask;
            }
            table[i] = encoded;
        }

        @Override
        protected int size(long bucketOrd) {
            final int state = state(bucketOrd);
            assert state >= 0 : "bucket is in HyperLogLog mode";
            return state;
        }

        @Override
        protected HashesIterator values(long bucketOrd) {
            final int size = size(bucketOrd);
            if (size == 1) {
                iterator.resetSingle(firstHashes.get(bucketOrd));
            } else {
                iterator.reset(size == 0 ? null : tables.get(bucketOrd), size);
            }
            return iterator;
        }
    }

    /**
     * Iterates over the hashes of one bucket: its hash table, or its single inline hash.
     */
    private static class LinearCountingIterator implements AbstractLinearCounting.HashesIterator {
        private int[] table;
        private int size;
        private int pos;
        private int value;
        private int single;

        void reset(int[] table, int size) {
            this.table = table;
            this.size = size;
            this.pos = 0;
            this.single = 0;
        }

        void resetSingle(int hash) {
            this.table = null;
            this.size = 1;
            this.pos = 0;
            this.single = hash;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean next() {
            if (table == null) {
                if (single != 0 && pos == 0) {
                    pos = 1;
                    value = single;
                    return true;
                }
                return false;
            }
            while (pos < table.length) {
                final int v = table[pos++];
                if (v != 0) {
                    value = v;
                    return true;
                }
            }
            return false;
        }

        @Override
        public int value() {
            return value;
        }
    }

    /**
     * HyperLogLog over the register slots. Its bucket ordinals are slots, not the buckets of the enclosing sketch.
     */
    private class HyperLogLog extends AbstractHyperLogLog {

        private final RegisterIterator iterator = new RegisterIterator();

        HyperLogLog(int precision) {
            super(precision);
        }

        @Override
        protected void addRunLen(long slot, int register, int runLen) {
            final long index = (slot << p) + register;
            // Unconditional, like HyperLogLogPlusPlus: whether a register grows is too unpredictable to branch on
            registers.set(index, (byte) Math.max(runLen, registers.get(index)));
            registerChanges++;
        }

        @Override
        protected RunLenIterator getRunLens(long slot) {
            iterator.reset(slot << p);
            return iterator;
        }

        private class RegisterIterator implements RunLenIterator {
            private long start;
            private int pos;
            private byte value;

            void reset(long start) {
                this.start = start;
                this.pos = 0;
            }

            @Override
            public boolean next() {
                if (pos < m) {
                    value = registers.get(start + pos);
                    pos++;
                    return true;
                }
                return false;
            }

            @Override
            public byte value() {
                return value;
            }
        }
    }
}
