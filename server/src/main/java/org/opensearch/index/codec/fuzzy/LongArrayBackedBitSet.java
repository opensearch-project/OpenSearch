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
public class LongArrayBackedBitSet implements Accountable, Closeable {

    private long underlyingArrayLength = 0L;
    private LongArray longArray;

    LongArrayBackedBitSet(long capacity) {
        underlyingArrayLength = ((capacity - 1L) >> 6) + 1;
        this.longArray = BigArrays.NON_RECYCLING_INSTANCE.withCircuitBreaking().newLongArray(underlyingArrayLength);
    }

    LongArrayBackedBitSet(IndexInput in) throws IOException {
        underlyingArrayLength = in.readLong();
        long streamLength = underlyingArrayLength << 3;
        this.longArray = new IndexInputLongArray(underlyingArrayLength, in.randomAccessSlice(in.getFilePointer(), streamLength));
        in.skipBytes(streamLength);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(underlyingArrayLength);
        for (int idx = 0; idx < underlyingArrayLength; idx++) {
            out.writeLong(longArray.get(idx));
        }
    }

    public long cardinality() {
        long tot = 0;
        for (int i = 0; i < underlyingArrayLength; ++i) {
            tot += Long.bitCount(longArray.get(i));
        }
        return tot;
    }

    public boolean isSet(long index) {
        long i = index >> 6; // div 64
        long val = longArray.get(i);
        // signed shift will keep a negative index and force an
        // array-index-out-of-bounds-exception, removing the need for an explicit check.
        long bitmask = 1L << index;
        return (val & bitmask) != 0;
    }

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
