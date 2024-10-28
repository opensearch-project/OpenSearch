/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.util.ByteArrayBackedBitset;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Helper class to read/write bitset for null values and identity values.
 */
public class StarTreeDocumentBitSetUtil {

    /**
     * Set identity values based on bitset.
     */
    public static int readBitSet(RandomAccessInput input, long offset, Object[] array, Function<Integer, Object> identityValueSupplier)
        throws IOException {
        ByteArrayBackedBitset bitset = new ByteArrayBackedBitset(input, offset, getLength(array));
        for (int i = 0; i < array.length; i++) {
            if (bitset.get(i)) {
                array[i] = identityValueSupplier.apply(i);
            }
        }
        return bitset.getCurrBytesRead();
    }

    /**
     * Write the bitset for the given array to the ByteBuffer
     */
    public static void writeBitSet(Object[] array, ByteBuffer buffer) throws IOException {
        ByteArrayBackedBitset bitset = new ByteArrayBackedBitset(getLength(array));
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null) {
                bitset.set(i);
            }
        }
        bitset.write(buffer);
    }

    private static int getLength(Object[] array) {
        return (array.length / 8) + (array.length % 8 == 0 ? 0 : 1);
    }
}
