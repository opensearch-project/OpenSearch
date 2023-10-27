/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.Assertions;

import java.io.IOException;
import java.util.Iterator;

/**
 * The code is based on Lucene's implementation of Bloom Filter.
 * It represents a subset of the Lucene implementation needed for OpenSearch use cases.
 * Since the Lucene implementation is marked experimental,
 * this aims to ensure we can provide a bwc implementation during upgrades.
 */
public class BloomFilter extends AbstractFuzzySet {

    private static final Logger logger = LogManager.getLogger(BloomFilter.class);

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

    private final LongArrayBackedBitSet bitset;
    private final int setSize;
    private final int hashCount;

    BloomFilter(long maxDocs, double maxFpp, CheckedSupplier<Iterator<BytesRef>, IOException> fieldIteratorProvider) throws IOException {
        int setSize = (int) Math.ceil((maxDocs * Math.log(maxFpp)) / Math.log(1 / Math.pow(2, Math.log(2))));
        setSize = getNearestSetSize(2 * setSize);
        int optimalK = (int) Math.round(((double) setSize / maxDocs) * Math.log(2));
        this.bitset = new LongArrayBackedBitSet(setSize + 1);
        this.setSize = setSize;
        this.hashCount = optimalK;
        addAll(fieldIteratorProvider);
        if (Assertions.ENABLED) {
            assertAllElementsExist(fieldIteratorProvider);
        }
        logger.trace("Bloom filter created with fpp: {}, setSize: {}, hashCount: {}", maxFpp, setSize, hashCount);
    }

    BloomFilter(IndexInput in) throws IOException {
        hashCount = in.readInt();
        setSize = in.readInt();
        this.bitset = new LongArrayBackedBitSet(in);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(hashCount);
        out.writeInt(setSize);
        bitset.writeTo(out);
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
        long hash = generateKey(value);
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

    protected void add(BytesRef value) {
        long hash = generateKey(value);
        int msb = (int) (hash >>> Integer.SIZE);
        int lsb = (int) hash;
        for (int i = 0; i < hashCount; i++) {
            // Bitmasking using bloomSize is effectively a modulo operation since set sizes are always power of 2
            int bloomPos = (lsb + i * msb) & setSize;
            bitset.set(bloomPos);
        }
    }

    @Override
    public boolean isSaturated() {
        long numBitsSet = bitset.cardinality();
        return (float) numBitsSet / (float) setSize > 0.9f;
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.sizeOf(bitset.ramBytesUsed());
    }

    private boolean mayContainValue(int aHash) {
        // Bloom sizes are always base 2 and so can be ANDed for a fast modulo
        int pos = aHash & setSize;
        return bitset.isSet(pos);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(bitset);
    }
}
