/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;

/**
 * A bitset backed by a long-indexed array.
 */
class LongArrayBackedBitSet implements Accountable, Closeable {

    private long underlyingArrayLength = 0L;
    private LongArray longArray;

    /**
     * Constructor which uses an on heap array. This should be using during construction of the bitset.
     * @param capacity The maximum capacity to provision for the bitset.
     */
    LongArrayBackedBitSet(long capacity) {
        // Since the bitset is backed by a long array, we only need 1 element for every 64 bits in the underlying array.
        underlyingArrayLength = (capacity >> 6) + 1L;
        this.longArray = BigArrays.NON_RECYCLING_INSTANCE.withCircuitBreaking().newLongArray(underlyingArrayLength);
    }

    /**
     * Constructor which uses Lucene's IndexInput to read the bitset into a read-only buffer.
     * @param in IndexInput containing the serialized bitset.
     * @throws IOException I/O exception
     */
    LongArrayBackedBitSet(IndexInput in) throws IOException {
        underlyingArrayLength = in.readLong();
        // Multiplying by 8 since the length above is of the long array, so we will have
        // 8 times the number of bytes in our stream.
        long streamLength = underlyingArrayLength << 3;
        this.longArray = new IndexInputImmutableLongArray(underlyingArrayLength, in.randomAccessSlice(in.getFilePointer(), streamLength));
        in.skipBytes(streamLength);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(underlyingArrayLength);
        for (int idx = 0; idx < underlyingArrayLength; idx++) {
            out.writeLong(longArray.get(idx));
        }
    }

    /**
     * This is an O(n) operation, and will iterate over all the elements in the underlying long array
     * to determine cardinality of the set.
     * @return number of set bits in the bitset.
     */
    public long cardinality() {
        long tot = 0;
        for (int i = 0; i < underlyingArrayLength; ++i) {
            tot += Long.bitCount(longArray.get(i));
        }
        return tot;
    }

    /**
     * Retrieves whether the bit is set or not at the given index.
     * @param index the index to look up for the bit
     * @return true if bit is set, false otherwise
     */
    public boolean get(long index) {
        long i = index >> 6; // div 64
        long val = longArray.get(i);
        long bitmask = 1L << index;
        return (val & bitmask) != 0;
    }

    /**
     * Sets the bit at the given index.
     * @param index the index to set the bit at.
     */
    public void set(long index) {
        long wordNum = index >> 6; // div 64
        long bitmask = 1L << index;
        long val = longArray.get(wordNum);
        longArray.set(wordNum, val | bitmask);
    }

    @Override
    public long ramBytesUsed() {
        return 128L + longArray.ramBytesUsed();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(longArray);
    }
}
