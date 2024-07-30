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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Helper class to read/write bitset for null values and identity values.
 */
public class StarTreeDocumentBitSetUtil {
    /**
     * Write bitset for null values.
     *
     * @param array array of objects
     * @param output output stream
     * @return number of bytes written
     * @throws IOException if an I/O error occurs while writing to the output stream
     */
    public static int writeBitSet(Object[] array, IndexOutput output) throws IOException {
        int numBytes = 0;
        List<Byte> nullBitSetList = new ArrayList<>();
        byte nullBitSet = 0;
        for (int i = 0; i < array.length; i++) {
            if (i % 8 == 0 && i > 0) {
                nullBitSetList.add(nullBitSet);
                nullBitSet = 0;
            }
            if (array[i] == null) {
                // Set the corresponding bit in dimensionNullBitSet to 1 (present)
                nullBitSet |= (byte) (1 << (i % 8));
            }
        }
        nullBitSetList.add(nullBitSet);
        for (Byte bitSet : nullBitSetList) {
            output.writeByte(bitSet);
            numBytes += Byte.BYTES;
        }
        return numBytes;
    }

    /**
     * Set null values based on bitset.
     */
    public static int readAndSetNullBasedOnBitSet(RandomAccessInput input, long offset, Object[] array) throws IOException {
        int numBytes = 0;
        byte nullDimensionsBitSet = input.readByte(offset + numBytes);
        numBytes += Byte.BYTES;
        for (int i = 0; i < array.length; i++) {
            if (i > 0 && i % 8 == 0) {
                nullDimensionsBitSet = input.readByte(offset + numBytes);
                numBytes += Byte.BYTES;
            }
            boolean isElementNull = (nullDimensionsBitSet & (1L << (i % 8))) != 0;
            if (isElementNull) {
                array[i] = null;
            }
        }
        return numBytes;
    }

    /**
     * Set identity values based on bitset.
     */
    public static int readAndSetIdentityValueBasedOnBitSet(
        RandomAccessInput input,
        long offset,
        Object[] array,
        Function<Integer, Object> identityValueSupplier
    ) throws IOException {
        int numBytes = 0;
        byte nullDimensionsBitSet = input.readByte(offset + numBytes);
        numBytes += Byte.BYTES;
        for (int i = 0; i < array.length; i++) {
            if (i > 0 && i % 8 == 0) {
                nullDimensionsBitSet = input.readByte(offset + numBytes);
                numBytes += Byte.BYTES;
            }
            boolean isElementNull = (nullDimensionsBitSet & (1L << (i % 8))) != 0;
            if (isElementNull) {
                array[i] = identityValueSupplier.apply(i);
            }
        }
        return numBytes;
    }
}
