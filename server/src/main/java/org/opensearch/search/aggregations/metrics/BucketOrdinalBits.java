/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.Nullable;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.IntArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.BucketOrdinalBitmaps;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.OrdinalsCollector;

import java.io.IOException;
import java.util.Arrays;

final class BucketOrdinalBits implements BucketOrdinalSet {
    /** Ordinals hashed at a time when hashing some of the buckets. */
    static final int HASH_CHUNK = 8192;
    static final int SPARSE_SHARE = 8;
    static final int SPARSE_DENSITY = 16;

    private final BigArrays bigArrays;
    private final int maxOrd;
    private final long memoryThreshold;
    private ObjectArray<BitArray> bits;
    /** Number of ordinals of each bucket, {@code null} when not counting them. */
    @Nullable
    private IntArray ordinalCounts;
    /** Total of {@link #ordinalCounts}. */
    private long ordinals;
    private long bucketsWithBits;
    private long memoryUsed;

    BucketOrdinalBits(BigArrays bigArrays, long maxOrd, long memoryThreshold, boolean countOrdinals) {
        if (maxOrd > Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        this.bigArrays = bigArrays;
        this.maxOrd = (int) maxOrd;
        this.memoryThreshold = memoryThreshold;
        boolean success = false;
        try {
            bits = bigArrays.newObjectArray(1);
            if (countOrdinals) {
                ordinalCounts = bigArrays.newIntArray(1, true);
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    /**
     * The bits of {@code bucket}, or {@code null} if the bucket has none and giving it some would take the memory used over
     * the threshold, or over its sparse share when counting sparse ordinals.
     */
    @Nullable
    BitArray bucket(long bucket) {
        if (bucket >= bits.size()) {
            bits = bigArrays.grow(bits, bucket + 1);
        }
        BitArray bucketBits = bits.get(bucket);
        if (bucketBits == null) {
            final long overhead = OrdinalsCollector.memoryOverhead(maxOrd);
            if (memoryUsed + overhead > memoryThreshold) {
                return null;
            }
            if (ordinalCounts != null) {
                if (memoryUsed + overhead > memoryThreshold / SPARSE_SHARE && ordinals * SPARSE_DENSITY < bucketsWithBits * maxOrd) {
                    return null;
                }
                ordinalCounts = bigArrays.grow(ordinalCounts, bucket + 1);
            }
            bucketBits = new BitArray(maxOrd, bigArrays);
            bits.set(bucket, bucketBits);
            bucketsWithBits++;
            memoryUsed += overhead;
        }
        return bucketBits;
    }

    /**
     * Adds {@code ord} to {@code bucketBits}, the bits of {@code bucket}, counting it if it is new. Only when counting.
     */
    void addCounted(long bucket, BitArray bucketBits, long ord) {
        if (bucketBits.get(ord) == false) {
            bucketBits.set(ord);
            ordinalCounts.increment(bucket, 1);
            ordinals++;
        }
    }

    /**
     * Number of distinct ordinals of {@code bucket}. Only when counting.
     */
    @Override
    public long cardinality(long bucket) {
        return bucket < ordinalCounts.size() ? ordinalCounts.get(bucket) : 0;
    }

    /**
     * Memory used by the bits, to compare with the threshold. {@link BigArrays} charges them to the circuit breaker.
     */
    long memoryUsed() {
        return memoryUsed;
    }

    private long nextOrd(BitArray bucketBits, long ord) {
        return ord + 1 < maxOrd ? bucketBits.nextSetBit(ord + 1) : Long.MAX_VALUE;
    }

    /**
     * Hashes every distinct ordinal once, then adds the hashes of each bucket's ordinals to that bucket of {@code counts}.
     *
     * @param values resolves the ordinals to their values
     */
    void collectInto(AbstractHyperLogLogPlusPlus counts, SortedSetDocValues values) throws IOException {
        collectAllInto(counts, values);
    }

    /**
     * Whether a bucket without bits can't get any because they would take the memory used over the threshold.
     */
    boolean isFull() {
        return memoryUsed + OrdinalsCollector.memoryOverhead(maxOrd) > memoryThreshold;
    }

    /**
     * Number of buckets that have bits.
     */
    long bucketsWithBits() {
        return bucketsWithBits;
    }

    /**
     * Estimated heap used by {@link #collectInto(AbstractHyperLogLogPlusPlus, SortedSetDocValues, long[])} for {@code buckets}
     * buckets, which it doesn't charge to the circuit breaker itself.
     */
    static long collectIntoBytes(long buckets) {
        return buckets * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2L * Long.BYTES) + (HASH_CHUNK + HASH_CHUNK / Long.SIZE)
            * (long) Long.BYTES;
    }

    /**
     * Every bucket: like {@link OrdinalsCollector} has always done, each visited ordinal's hash is kept by ordinal.
     */
    private void collectAllInto(AbstractHyperLogLogPlusPlus counts, SortedSetDocValues values) throws IOException {
        try (BitArray allVisitedOrds = new BitArray(maxOrd, bigArrays)) {
            for (long bucket = bits.size() - 1; bucket >= 0; --bucket) {
                final BitArray bucketBits = bits.get(bucket);
                if (bucketBits != null) {
                    allVisitedOrds.or(bucketBits);
                }
            }

            try (LongArray hashes = bigArrays.newLongArray(maxOrd, false)) {
                final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                for (long ord = allVisitedOrds.nextSetBit(0); ord < Long.MAX_VALUE; ord = nextOrd(allVisitedOrds, ord)) {
                    final BytesRef value = values.lookupOrd(ord);
                    MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes.set(ord, hash.h1);
                }

                for (long bucket = bits.size() - 1; bucket >= 0; --bucket) {
                    final BitArray bucketBits = bits.get(bucket);
                    if (bucketBits != null) {
                        for (long ord = bucketBits.nextSetBit(0); ord < Long.MAX_VALUE; ord = nextOrd(bucketBits, ord)) {
                            counts.collect(bucket, hashes.get(ord));
                        }
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Every distinct ordinal is looked up once and in ascending order, which is the cheapest way to read a terms dictionary,
     * {@value #HASH_CHUNK} ordinals at a time so that only the hashes of one chunk are held, whatever the number of ordinals.
     */
    @Override
    public void collectInto(AbstractHyperLogLogPlusPlus counts, SortedSetDocValues values, @Nullable long[] buckets) throws IOException {
        if (buckets == null) {
            buckets = new long[Math.toIntExact(bucketsWithBits)];
            int n = 0;
            for (long bucket = 0; bucket < bits.size(); bucket++) {
                if (bits.get(bucket) != null) {
                    buckets[n++] = bucket;
                }
            }
        }
        final BitArray[] selected = new BitArray[buckets.length];
        final long[] selectedBuckets = new long[buckets.length];
        // the next ordinal of each selected bucket to hash
        final long[] next = new long[buckets.length];
        int n = 0;
        for (long bucket : buckets) {
            final BitArray bucketBits = bucket < bits.size() ? bits.get(bucket) : null;
            final long first = bucketBits == null ? Long.MAX_VALUE : bucketBits.nextSetBit(0);
            if (first < maxOrd) {
                selected[n] = bucketBits;
                selectedBuckets[n] = bucket;
                next[n] = first;
                n++;
            }
        }
        final long[] chunk = new long[HASH_CHUNK / Long.SIZE];
        final long[] hashes = new long[HASH_CHUNK];
        final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        while (true) {
            long min = Long.MAX_VALUE;
            for (int i = 0; i < n; i++) {
                min = Math.min(min, next[i]);
            }
            if (min == Long.MAX_VALUE) {
                return;
            }
            final long start = min - min % HASH_CHUNK;
            final long end = start + HASH_CHUNK;
            Arrays.fill(chunk, 0L);
            for (int i = 0; i < n; i++) {
                for (long ord = next[i]; ord < end; ord = nextOrd(selected[i], ord)) {
                    final int offset = (int) (ord - start);
                    chunk[offset >>> 6] |= 1L << offset;
                }
            }
            for (int w = 0; w < chunk.length; w++) {
                for (long word = chunk[w]; word != 0; word &= word - 1) {
                    final int offset = (w << 6) | Long.numberOfTrailingZeros(word);
                    final BytesRef value = values.lookupOrd(start + offset);
                    MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes[offset] = hash.h1;
                }
            }
            for (int i = 0; i < n; i++) {
                long ord = next[i];
                for (; ord < end; ord = nextOrd(selected[i], ord)) {
                    counts.collect(selectedBuckets[i], hashes[(int) (ord - start)]);
                }
                next[i] = ord;
            }
        }
    }

    /**
     * Adds the ordinals of every bucket to the same bucket of {@code bitmaps}.
     */
    void copyTo(BucketOrdinalBitmaps bitmaps) {
        for (long bucket = 0; bucket < bits.size(); bucket++) {
            final BitArray bucketBits = bits.get(bucket);
            if (bucketBits != null) {
                for (long ord = bucketBits.nextSetBit(0); ord < Long.MAX_VALUE; ord = nextOrd(bucketBits, ord)) {
                    bitmaps.add(bucket, (int) ord);
                }
            }
        }
        bitmaps.flushBreaker();
    }

    @Override
    public void close() {
        if (bits != null) {
            for (long i = 0; i < bits.size(); i++) {
                Releasables.close(bits.get(i));
            }
        }
        Releasables.close(bits, ordinalCounts);
    }
}
