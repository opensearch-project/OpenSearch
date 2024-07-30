/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Unit tests for {@link StarTreeDocumentBitSetUtil}
 */
public class StarTreeDocumentBitSetUtilTests extends OpenSearchTestCase {

    public void testWriteAndReadNullBitSets() throws IOException {
        for (int k = 0; k < 10; k++) {
            int randomArraySize = randomIntBetween(2, 256);
            Long[] dims = new Long[randomArraySize];
            for (int i = 0; i < randomArraySize; i++) {
                dims[i] = randomLong();
            }
            testNullBasedOnBitset(dims);
        }
    }

    void testNullBasedOnBitset(Long[] dims) throws IOException {
        Long[] dims1 = Arrays.copyOf(dims, dims.length);
        int randomNullIndex1 = randomIntBetween(0, dims.length - 1);
        int randomNullIndex2 = randomIntBetween(0, dims.length - 1);
        dims[randomNullIndex1] = null;
        dims[randomNullIndex2] = null;
        Path basePath = createTempDir("OffHeapTests");
        FSDirectory fsDirectory = FSDirectory.open(basePath);
        String TEST_FILE = "test_file";
        IndexOutput indexOutput = fsDirectory.createOutput(TEST_FILE, IOContext.DEFAULT);
        StarTreeDocumentBitSetUtil.writeBitSet(dims, indexOutput);
        indexOutput.close();

        // test null value on read
        IndexInput in = fsDirectory.openInput(TEST_FILE, IOContext.DEFAULT);
        RandomAccessInput randomAccessInput = in.randomAccessSlice(0, in.length());
        StarTreeDocumentBitSetUtil.readAndSetNullBasedOnBitSet(randomAccessInput, 0, dims1);
        assertNull(dims1[randomNullIndex1]);
        assertNull(dims1[randomNullIndex2]);
        in.close();

        // test identity value on read
        long randomLong = randomLong();
        Function<Integer, Object> identityValueSupplier = i -> randomLong;
        in = fsDirectory.openInput(TEST_FILE, IOContext.DEFAULT);

        randomAccessInput = in.randomAccessSlice(0, in.length());
        StarTreeDocumentBitSetUtil.readAndSetIdentityValueBasedOnBitSet(randomAccessInput, 0, dims1, identityValueSupplier);
        assertEquals(randomLong, (long) dims1[randomNullIndex1]);
        assertEquals(randomLong, (long) dims1[randomNullIndex2]);
        in.close();
    }
}
