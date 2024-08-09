/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;

/**
 * A bitset backed by a byte array. This will initialize and set bits in the byte array based on the index.
 */
public class ByteArrayBackedBitset {
    private final byte[] byteArray;

    /**
     * Constructor which uses an on heap list. This should be using during construction of the bitset.
     */
    public ByteArrayBackedBitset(int capacity) {
        byteArray = new byte[capacity];
    }

    /**
     * Constructor which set the Lucene's RandomAccessInput to read the bitset into a read-only buffer.
     */
    public ByteArrayBackedBitset(RandomAccessInput in, long offset, int length) throws IOException {
        byteArray = new byte[length];
        int i = 0;
        while (i < length) {
            byteArray[i] = in.readByte(offset + i);
            i++;
        }
    }

    /**
     * Constructor which set the Lucene's IndexInput to read the bitset into a read-only buffer.
     */
    public ByteArrayBackedBitset(IndexInput in, int length) throws IOException {
        byteArray = new byte[length];
        int i = 0;
        while (i < length) {
            byteArray[i] = in.readByte();
            i++;
        }
    }

    /**
     * Sets the bit at the given index to 1.
     * Each byte can indicate 8 bits, so the index is divided by 8 to get the byte array index.
     * @param index the index to set the bit
     */
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

    /**
     * Retrieves whether the bit is set or not at the given index.
     * @param index the index to look up for the bit
     * @return true if bit is set, false otherwise
     */
    public boolean get(int index) throws IOException {
        int byteArrIndex = index >> 3;
        return (byteArray[byteArrIndex] & (1 << (index & 7))) != 0;
    }

    public int getCurrBytesRead() {
        return byteArray.length;
    }
}
