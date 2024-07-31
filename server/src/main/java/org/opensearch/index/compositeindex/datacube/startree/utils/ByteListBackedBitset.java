/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;

/**
 * This is used to store and retrieve null values in the bitset.
 */
public class ByteListBackedBitset {
    private final byte[] byteArray;

    /**
     * Constructor which uses an on heap list. This should be using during construction of the bitset.
     */
    public ByteListBackedBitset(int capacity) {
        byteArray = new byte[capacity];
    }

    /**
     * Constructor which set the Lucene's IndexInput to read the bitset into a read-only buffer.
     */
    public ByteListBackedBitset(RandomAccessInput in, long offset, int length) throws IOException {
        byteArray = new byte[length];
        int i = 0;
        while (i < length) {
            byteArray[i] = in.readByte(offset + i);
            i++;
        }
    }

    public void set(int index) {
        int byteArrIndex = index >> 3;
        byteArray[byteArrIndex] |= (byte) (1 << (index & 7));
    }

    public int write(IndexOutput output) throws IOException {
        int numBytes = 0;
        for (Byte bitSet : byteArray) {
            output.writeByte(bitSet);
            numBytes += Byte.BYTES;
        }
        return numBytes;
    }

    public boolean get(int index) throws IOException {
        int byteArrIndex = index >> 3;
        return (byteArray[byteArrIndex] & (1 << (index & 7))) != 0;
    }

    public int getCurrBytesRead() {
        return byteArray.length;
    }
}
