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
        int length = (array.length / 8) + (array.length % 8 == 0 ? 0 : 1);
        ByteListBackedBitset bitset = new ByteListBackedBitset(length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null) {
                bitset.set(i);
            }
        }
        return bitset.write(output);
    }

    /**
     * Set identity values based on bitset.
     */
    public static int readBitSet(RandomAccessInput input, long offset, Object[] array, Function<Integer, Object> identityValueSupplier)
        throws IOException {
        ByteListBackedBitset bitset = new ByteListBackedBitset(input, offset, getLength(array));
        for (int i = 0; i < array.length; i++) {
            if (bitset.get(i)) {
                array[i] = identityValueSupplier.apply(i);
            }
        }
        return bitset.getCurrBytesRead();
    }

    private static int getLength(Object[] array) {
        return (array.length / 8) + (array.length % 8 == 0 ? 0 : 1);
    }
}
