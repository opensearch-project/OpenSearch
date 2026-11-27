/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

import org.roaringbitmap.ContainerPointer;
import org.roaringbitmap.PeekableCharIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * The rank of each value of a set of ordinals, its position among them in ascending order, so that the hashes of the values
 * can be kept in an array as long as the set rather than as long as the ordinal space. Like a {@link RoaringBitmap} it
 * splits the ordinals into blocks of 65536: a block holding more than {@value #DENSE_VALUES} values keeps them as bits with
 * the rank of each 64-bit word, so its ranks take constant time, and the others keep their sorted values, searched in at
 * most 12 steps. That costs at most about 3 bytes per value. Ranks are fastest when asked in ascending order, as the block
 * of the previous ordinal is tried first.
 *
 * @opensearch.internal
 */
final class VisitedRanks {
    /** Blocks with more values than this keep bits, as roaring bitmaps do. */
    static final int DENSE_VALUES = 4096;
    private static final int WORDS_PER_BLOCK = 1024;

    private final char[] keys;
    /** Rank of the first value of each block. */
    private final int[] blockRanks;
    /** Offset of a dense block in {@link #words}, or {@code -1 - offset} of a sparse one in {@link #lows}. */
    private final int[] offsets;
    private final long[] words;
    /** Rank of the first bit of each word within its block. */
    private final int[] wordRanks;
    private final char[] lows;
    private final int[] lowEnds;
    private int lastKey = -1;
    private int lastBlock;

    VisitedRanks(RoaringBitmap set) {
        final int blocks = set.getContainerCount();
        keys = new char[blocks];
        blockRanks = new int[blocks];
        offsets = new int[blocks];
        lowEnds = new int[blocks];
        int dense = 0;
        int sparseValues = 0;
        ContainerPointer pointer = set.getContainerPointer();
        for (; pointer.getContainer() != null; pointer.advance()) {
            if (pointer.getCardinality() > DENSE_VALUES) {
                dense++;
            } else {
                sparseValues += pointer.getCardinality();
            }
        }
        words = new long[dense * WORDS_PER_BLOCK];
        wordRanks = new int[dense * WORDS_PER_BLOCK];
        lows = new char[sparseValues];
        int rank = 0;
        int denseOffset = 0;
        int lowOffset = 0;
        pointer = set.getContainerPointer();
        for (int b = 0; pointer.getContainer() != null; pointer.advance(), b++) {
            keys[b] = pointer.key();
            blockRanks[b] = rank;
            final PeekableCharIterator values = pointer.getContainer().getCharIterator();
            if (pointer.getCardinality() > DENSE_VALUES) {
                offsets[b] = denseOffset;
                while (values.hasNext()) {
                    final char low = values.next();
                    words[denseOffset + (low >>> 6)] |= 1L << low;
                }
                int wordRank = 0;
                for (int w = 0; w < WORDS_PER_BLOCK; w++) {
                    wordRanks[denseOffset + w] = wordRank;
                    wordRank += Long.bitCount(words[denseOffset + w]);
                }
                denseOffset += WORDS_PER_BLOCK;
            } else {
                offsets[b] = -1 - lowOffset;
                while (values.hasNext()) {
                    lows[lowOffset++] = values.next();
                }
            }
            lowEnds[b] = lowOffset;
            rank += pointer.getCardinality();
        }
    }

    /**
     * Estimated heap used by the ranks of {@code set}.
     */
    static long ramBytesUsed(RoaringBitmap set) {
        long bytes = RamUsageEstimator.shallowSizeOfInstance(VisitedRanks.class) + 7L * RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        final ContainerPointer pointer = set.getContainerPointer();
        for (; pointer.getContainer() != null; pointer.advance()) {
            bytes += Character.BYTES + 3L * Integer.BYTES;
            if (pointer.getCardinality() > DENSE_VALUES) {
                bytes += (long) WORDS_PER_BLOCK * (Long.BYTES + Integer.BYTES);
            } else {
                bytes += (long) Character.BYTES * pointer.getCardinality();
            }
        }
        return bytes;
    }

    /**
     * The rank of {@code ord}, which must be in the set.
     */
    int rank(int ord) {
        final int key = ord >>> 16;
        if (key != lastKey) {
            lastBlock = Arrays.binarySearch(keys, (char) key);
            lastKey = key;
        }
        final int block = lastBlock;
        assert block >= 0 : "ordinal " + ord + " isn't in the set";
        final char low = (char) ord;
        final int offset = offsets[block];
        if (offset >= 0) {
            final int word = offset + (low >>> 6);
            return blockRanks[block] + wordRanks[word] + Long.bitCount(words[word] & ((1L << low) - 1));
        }
        final int start = -1 - offset;
        final int index = Arrays.binarySearch(lows, start, lowEnds[block], low);
        assert index >= 0 : "ordinal " + ord + " isn't in the set";
        return blockRanks[block] + index - start;
    }
}
