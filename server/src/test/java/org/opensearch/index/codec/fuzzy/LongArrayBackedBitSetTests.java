/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class LongArrayBackedBitSetTests extends OpenSearchTestCase {

    public void testBasicOperations() {
        LongArrayBackedBitSet bitSet = new LongArrayBackedBitSet(128);
        bitSet.set(0);
        bitSet.set(63);
        bitSet.set(64);
        bitSet.set(127);
        assertTrue(bitSet.get(0));
        assertTrue(bitSet.get(63));
        assertTrue(bitSet.get(64));
        assertTrue(bitSet.get(127));
        assertFalse(bitSet.get(1));
        assertFalse(bitSet.get(62));
        assertFalse(bitSet.get(65));
        assertFalse(bitSet.get(126));
    }

    public void testCardinality() {
        LongArrayBackedBitSet bitSet = new LongArrayBackedBitSet(256);
        assertEquals(0, bitSet.cardinality());
        bitSet.set(0);
        assertEquals(1, bitSet.cardinality());
        bitSet.set(63);
        assertEquals(2, bitSet.cardinality());
        bitSet.set(64);
        assertEquals(3, bitSet.cardinality());
        bitSet.set(64);  // Setting same bit again
        assertEquals(3, bitSet.cardinality());
    }

    public void testRandomOperations() {
        int capacity = randomIntBetween(64, 1024);
        LongArrayBackedBitSet bitSet = new LongArrayBackedBitSet(capacity);
        boolean[] setBits = new boolean[capacity];
        int expectedCardinality = 0;
        int operations = randomIntBetween(10, 100);
        for (int i = 0; i < operations; i++) {
            int index = randomIntBetween(0, capacity - 1);
            bitSet.set(index);
            if (!setBits[index]) {
                setBits[index] = true;
                expectedCardinality++;
            }
        }
        assertEquals(expectedCardinality, bitSet.cardinality());
        for (int i = 0; i < capacity; i++) {
            assertEquals(setBits[i], bitSet.get(i));
        }
    }

    public void testBloomFilterFunctionality() {
        int expectedItems = 1000;
        int bitArraySize = expectedItems * 10;
        LongArrayBackedBitSet bloomFilter = new LongArrayBackedBitSet(bitArraySize);
        int numHashes = 3;
        String[] addedItems = new String[expectedItems];
        for (int i = 0; i < expectedItems; i++) {
            addedItems[i] = "item" + i;
            long[] hashes = calculateHashes(addedItems[i], numHashes, bitArraySize);
            for (long hash : hashes) {
                bloomFilter.set(hash);
            }
        }
        int falseNegatives = 0;
        for (String item : addedItems) {
            boolean found = true;
            long[] hashes = calculateHashes(item, numHashes, bitArraySize);
            for (long hash : hashes) {
                if (!bloomFilter.get(hash)) {
                    found = false;
                    break;
                }
            }
            if (!found) {
                falseNegatives++;
            }
        }
        assertEquals("Bloom filter should have no false negatives", 0, falseNegatives);
        int testCount = 10000;
        int falsePositives = 0;
        for (int i = 0; i < testCount; i++) {
            String nonExistentItem = "nonexistent" + randomAlphaOfLength(10) + i;
            boolean found = true;
            long[] hashes = calculateHashes(nonExistentItem, numHashes, bitArraySize);
            for (long hash : hashes) {
                if (!bloomFilter.get(hash)) {
                    found = false;
                    break;
                }
            }
            if (found) {
                falsePositives++;
            }
        }
        double falsePositiveRate = (double) falsePositives / testCount;
        logger.info("False positive rate: {}", falsePositiveRate);
        assertTrue("False positive rate should be reasonable", falsePositiveRate < 0.2);
        long setBits = bloomFilter.cardinality();
        double fillRatio = (double) setBits / bitArraySize;
        logger.info("Bloom filter fill ratio: {}", fillRatio);
        assertTrue("Fill ratio should be reasonable (between 0.1 and 0.9)", fillRatio > 0.1 && fillRatio < 0.9);
    }

    private long[] calculateHashes(String item, int numHashes, int size) {
        long[] results = new long[numHashes];
        long h1 = item.hashCode();
        long h2 = h1 * 31;

        h1 = mixHash(h1);
        h2 = mixHash(h2);

        for (int i = 0; i < numHashes; i++) {
            results[i] = Math.abs((h1 + (i + 1) * h2) % size);
        }
        return results;
    }

    private long mixHash(long h) {
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    public void testBloomFilterSaturation() {
        int bitArraySize = 1024;
        LongArrayBackedBitSet bloomFilter = new LongArrayBackedBitSet(bitArraySize);
        int numHashes = 3;
        int itemsAdded = 0;
        while (bloomFilter.cardinality() < bitArraySize * 0.9 && itemsAdded < 10000) {
            String item = "test" + itemsAdded;
            long[] hashes = calculateHashes(item, numHashes, bitArraySize);
            for (long hash : hashes) {
                bloomFilter.set(hash);
            }
            itemsAdded++;
        }

        double fillRatio = (double) bloomFilter.cardinality() / bitArraySize;
        logger.info("Saturation test fill ratio: {} after {} items", fillRatio, itemsAdded);
        assertTrue("Should achieve significant fill ratio", fillRatio > 0.5);
    }

    public void testRamBytesUsed() {
        LongArrayBackedBitSet bitSet = new LongArrayBackedBitSet(128);
        assertTrue(bitSet.ramBytesUsed() > 0);
    }

    public void testClose() throws IOException {
        LongArrayBackedBitSet bitSet = new LongArrayBackedBitSet(128);
        bitSet.set(0);
        bitSet.close();
    }
}
