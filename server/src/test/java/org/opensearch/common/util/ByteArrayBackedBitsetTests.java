/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Tests for {@link ByteArrayBackedBitset}
 */
public class ByteArrayBackedBitsetTests extends OpenSearchTestCase {
    public void testWriteAndReadNullBitSets() throws IOException {
        for (int k = 0; k < 10; k++) {
            int randomArraySize = randomIntBetween(2, 300);
            int randomIndex1 = randomIntBetween(0, (randomArraySize - 1) * 8);
            int randomIndex2 = randomIntBetween(0, (randomArraySize - 1) * 8);
            testWriteAndReadBitset(randomArraySize, randomIndex1, randomIndex2);
        }
    }

    private static void testWriteAndReadBitset(int randomArraySize, int randomIndex1, int randomIndex2) throws IOException {
        ByteArrayBackedBitset bitset = new ByteArrayBackedBitset(randomArraySize);
        Path basePath = createTempDir("OffHeapTests");
        FSDirectory fsDirectory = FSDirectory.open(basePath);
        String TEST_FILE = "test_file";
        IndexOutput indexOutput = fsDirectory.createOutput(TEST_FILE, IOContext.DEFAULT);
        bitset.set(randomIndex1);
        bitset.set(randomIndex2);
        bitset.write(indexOutput);
        indexOutput.close();

        IndexInput in = fsDirectory.openInput(TEST_FILE, IOContext.DEFAULT);
        RandomAccessInput randomAccessInput = in.randomAccessSlice(0, in.length());
        ByteArrayBackedBitset bitset1 = new ByteArrayBackedBitset(randomAccessInput, 0, randomArraySize);
        for (int i = 0; i < (randomArraySize * 8); i++) {
            if (randomIndex1 == i || randomIndex2 == i) {
                assertTrue(bitset1.get(i));
            } else {
                assertFalse(bitset1.get(i));
            }
        }
    }
}
