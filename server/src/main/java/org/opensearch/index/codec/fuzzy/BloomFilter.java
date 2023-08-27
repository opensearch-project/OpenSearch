/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Optional;

/**
 * The code is based on Lucene's implementation of Bloom Filter.
 * It represents a subset of the Lucene implementation needed for OpenSearch use cases.
 * Since the Lucene implementation is marked experimental,
 * this aims to ensure we can provide a bwc implementation during upgrades.
 */
public class BloomFilter implements FuzzySet {

    // The sizes of BitSet used are all numbers that, when expressed in binary form,
    // are all ones. This is to enable fast downsizing from one bitset to another
    // by simply ANDing each set index in one bitset with the size of the target bitset
    // - this provides a fast modulo of the number. Values previously accumulated in
    // a large bitset and then mapped to a smaller set can be looked up using a single
    // AND operation of the query term's hash rather than needing to perform a 2-step
    // translation of the query term that mirrors the stored content's reprojections.
    static final int[] usableBitSetSizes;

    static {
        usableBitSetSizes = new int[26];
        for (int i = 0; i < usableBitSetSizes.length; i++) {
            usableBitSetSizes[i] = (1 << (i + 6)) - 1;
        }
    }

    private final FixedBitSet filter;
    private final int setSize;
    private final int hashCount;

    BloomFilter(DataInput in) throws IOException {
        int hashCount = in.readVInt();
        int setSize = in.readInt();
        int numLongs = in.readInt();
        long[] longs = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            longs[i] = in.readLong();
        }
        this.filter = new FixedBitSet(longs, setSize + 1);
        this.setSize = setSize;
        this.hashCount = hashCount;
    }

    private BloomFilter(FixedBitSet filter, int setSize, int hashCount) {
        this.filter = filter;
        this.setSize = setSize;
        this.hashCount = hashCount;
    }

    public BloomFilter(int maxDocs, double maxFpp) {
        int setSize =
            (int)
                Math.ceil(
                    (maxDocs * Math.log(maxFpp))
                        / Math.log(1 / Math.pow(2, Math.log(2))));
        setSize = getNearestSetSize(2 * setSize);
        int optimalK = (int) Math.round(((double) setSize / maxDocs) * Math.log(2));

        this.filter = new FixedBitSet(setSize + 1);
        this.setSize = setSize;
        this.hashCount = optimalK;
    }

    static int getNearestSetSize(int maxNumberOfBits) {
        int result = usableBitSetSizes[0];
        for (int i = 0; i < usableBitSetSizes.length; i++) {
            if (usableBitSetSizes[i] <= maxNumberOfBits) {
                result = usableBitSetSizes[i];
            }
        }
        return result;
    }

    @Override
    public SetType setType() {
        return SetType.BLOOM_FILTER_V1;
    }

    @Override
    public Result contains(BytesRef value) {
        long hash = MurmurHash64.INSTANCE.hash(value);
        int msb = (int) (hash >>> Integer.SIZE);
        int lsb = (int) hash;
        for (int i = 0; i < hashCount; i++) {
            int bloomPos = (lsb + i * msb);
            if (!mayContainValue(bloomPos)) {
                return Result.NO;
            }
        }
        return Result.MAYBE;
    }

    public void add(BytesRef value) {
        long hash = MurmurHash64.INSTANCE.hash(value);
        int msb = (int) (hash >>> Integer.SIZE);
        int lsb = (int) hash;
        for (int i = 0; i < hashCount; i++) {
            // Bitmasking using bloomSize is effectively a modulo operation since set sizes are always power of 2
            int bloomPos = (lsb + i * msb) & setSize;
            filter.set(bloomPos);
        }
    }

    @Override
    public boolean isSaturated() {
        int numBitsSet = filter.cardinality();
        return (float) numBitsSet / (float) setSize > 0.9f;
    }

    @Override
    public Optional<FuzzySet> maybeDownsize() {
        return Optional.empty();
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeVInt(hashCount);
        out.writeInt(setSize);
        long[] bits = filter.getBits();
        out.writeInt(bits.length);
        for (int i = 0; i < bits.length; i++) {
            // Can't used VLong encoding because cant cope with negative numbers
            // output by FixedBitSet
            out.writeLong(bits[i]);
        }
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.sizeOf(filter.getBits());
    }

    private boolean mayContainValue(int aHash) {
        // Bloom sizes are always base 2 and so can be ANDed for a fast modulo
        int pos = aHash & setSize;
        return filter.get(pos);
    }
}
