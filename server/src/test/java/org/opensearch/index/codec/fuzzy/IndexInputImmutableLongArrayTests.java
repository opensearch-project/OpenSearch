/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.OpenSearchException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class IndexInputImmutableLongArrayTests extends OpenSearchTestCase {

    public void testBasicOperations() {
        long[] testData = { 1L, 2L, 3L, 4L, 5L };
        IndexInputImmutableLongArray array = createTestArray(testData);
        assertEquals(testData.length, array.size());
        for (int i = 0; i < testData.length; i++) {
            assertEquals(testData[i], array.get(i));
        }
    }

    public void testRandomAccess() {
        long[] testData = new long[1000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = randomLong();
        }
        IndexInputImmutableLongArray array = createTestArray(testData);
        for (int i = 0; i < 100; i++) {
            int randomIndex = randomIntBetween(0, testData.length - 1);
            assertEquals(testData[randomIndex], array.get(randomIndex));
        }
    }

    public void testBloomFilterFunctionality() throws IOException {
        int expectedItems = 1000;
        int bitsPerItem = 10;
        int bitArraySize = expectedItems * bitsPerItem;
        int numHashes = 7;
        int longArraySize = (bitArraySize + 63) / 64;
        long[] bitArray = new long[longArraySize];
        IndexInputImmutableLongArray bloomFilterArray = createTestArray(bitArray);
        String[] addedItems = new String[expectedItems];
        for (int i = 0; i < expectedItems; i++) {
            addedItems[i] = "item" + i;
            int[] hashPositions = calculateHashes(addedItems[i], numHashes, bitArraySize);
            for (int pos : hashPositions) {
                pos = pos % bitArraySize;
                int longIndex = pos >>> 6;
                long bitMask = 1L << (pos & 63);
                bitArray[longIndex] |= bitMask;
            }
        }
        int falseNegatives = 0;
        for (String item : addedItems) {
            boolean found = true;
            int[] hashPositions = calculateHashes(item, numHashes, bitArraySize);
            for (int pos : hashPositions) {
                int longIndex = pos >>> 6;
                long bitMask = 1L << (pos & 63);
                if ((bloomFilterArray.get(longIndex) & bitMask) == 0) {
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
            int[] hashPositions = calculateHashes(nonExistentItem, numHashes, bitArraySize);
            for (int pos : hashPositions) {
                int longIndex = pos >>> 6;
                long bitMask = 1L << (pos & 63);
                if ((bloomFilterArray.get(longIndex) & bitMask) == 0) {
                    found = false;
                    break;
                }
            }
            if (found) {
                falsePositives++;
            }
        }
        double falsePositiveRate = (double) falsePositives / testCount;
        logger.info("Bloom filter false positive rate: {}", falsePositiveRate);
        assertTrue("False positive rate should be reasonable (< 0.1)", falsePositiveRate < 0.1);
        long setBits = countSetBits(bloomFilterArray, bitArray.length);
        double fillRatio = (double) setBits / (bitArraySize);
        logger.info("Bloom filter fill ratio: {}", fillRatio);
        assertTrue("Fill ratio should be reasonable", fillRatio > 0.1 && fillRatio < 0.9);
    }

    private long countSetBits(IndexInputImmutableLongArray array, int length) {
        long count = 0;
        for (int i = 0; i < length; i++) {
            count += Long.bitCount(array.get(i));
        }
        return count;
    }

    private int[] calculateHashes(String item, int numHashes, int size) {
        int[] positions = new int[numHashes];
        long hash1 = mixHash(item.hashCode());
        long hash2 = mixHash(hash1);

        for (int i = 0; i < numHashes; i++) {
            long combinedHash = hash1 + i * hash2;
            positions[i] = (int) (Math.abs(combinedHash % size));
        }
        return positions;
    }

    private long mixHash(long h) {
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    public void testBloomFilterPerformance() throws IOException {
        int expectedItems = 10_000;
        int bitsPerItem = 10;
        int bitArraySize = expectedItems * bitsPerItem;
        int numHashes = 7;

        int longArraySize = (bitArraySize + 63) / 64;
        long[] bitArray = new long[longArraySize];
        IndexInputImmutableLongArray bloomFilterArray = createTestArray(bitArray);
        for (int i = 0; i < expectedItems; i++) {
            String item = "item" + i;
            int[] hashPositions = calculateHashes(item, numHashes, bitArraySize);
            for (int pos : hashPositions) {
                pos = pos % bitArraySize;
                int longIndex = pos >>> 6;
                long bitMask = 1L << (pos & 63);
                bitArray[longIndex] |= bitMask;
            }
        }
        int lookups = 1000;
        for (int i = 0; i < lookups; i++) {
            String item = "item" + randomIntBetween(0, expectedItems - 1);
            boolean found = true;
            int[] hashPositions = calculateHashes(item, numHashes, bitArraySize);
            for (int pos : hashPositions) {
                int longIndex = pos >>> 6;
                long bitMask = 1L << (pos & 63);
                if ((bloomFilterArray.get(longIndex) & bitMask) == 0) {
                    found = false;
                    break;
                }
            }
            assertTrue("Should find existing item", found);
        }
        logger.debug("Bloom filter operation completed successfully with {} items and {} lookups", expectedItems, lookups);
    }

    public void testUnsupportedOperations() {
        IndexInputImmutableLongArray array = createTestArray(new long[] { 1L, 2L, 3L });
        expectThrows(UnsupportedOperationException.class, () -> array.set(0, 42L));
        expectThrows(UnsupportedOperationException.class, () -> array.increment(0, 1L));
        expectThrows(UnsupportedOperationException.class, () -> array.fill(0, 2, 42L));
    }

    public void testInvalidAccess() {
        IndexInputImmutableLongArray array = createTestArray(new long[] { 1L, 2L, 3L });
        expectThrows(OpenSearchException.class, () -> array.get(-1));
        expectThrows(OpenSearchException.class, () -> array.get(array.size()));
    }

    public void testArrayWithBloomFilter() throws IOException {
        int elementCount = 1000;
        long[] bitArray = new long[elementCount];
        for (int i = 0; i < elementCount; i++) {
            bitArray[i] = randomLong();
        }
        IndexInputImmutableLongArray array = createTestArray(bitArray);
        assertEquals(elementCount, array.size());
        for (int i = 0; i < 100; i++) {
            int randomIndex = randomIntBetween(0, elementCount - 1);
            assertEquals(bitArray[randomIndex], array.get(randomIndex));
        }
        expectThrows(UnsupportedOperationException.class, () -> array.set(0, randomLong()));
    }

    public void testArrayBoundaries() throws IOException {
        int elementCount = 100;
        long[] bitArray = new long[elementCount];
        IndexInputImmutableLongArray array = createTestArray(bitArray);
        assertEquals("First element should be accessible", bitArray[0], array.get(0));
        assertEquals("Last element should be accessible", bitArray[elementCount - 1], array.get(elementCount - 1));
        expectThrows(OpenSearchException.class, () -> array.get(-1));
        expectThrows(OpenSearchException.class, () -> array.get(elementCount));
    }

    public void testLargeArrayPerformance() throws IOException {
        int elementCount = 100_000;
        long[] bitArray = new long[elementCount];
        for (int i = 0; i < elementCount; i++) {
            bitArray[i] = randomLong();
        }
        IndexInputImmutableLongArray array = createTestArray(bitArray);
        long startTime = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            int randomIndex = randomIntBetween(0, elementCount - 1);
            assertEquals(bitArray[randomIndex], array.get(randomIndex));
        }
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        logger.info("Random access time for {} elements: {} ns", elementCount, duration);
    }

    public void testConcurrentAccess() throws Exception {
        int elementCount = 10_000;
        long[] bitArray = new long[elementCount];
        for (int i = 0; i < elementCount; i++) {
            bitArray[i] = randomLong();
        }

        final IndexInputImmutableLongArray array = createTestArray(bitArray);
        Thread[] threads = new Thread[3];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    int randomIndex = randomIntBetween(0, elementCount - 1);
                    assertEquals(bitArray[randomIndex], array.get(randomIndex));
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testEmptyArray() {
        IndexInputImmutableLongArray array = createTestArray(new long[] {});
        assertEquals(0, array.size());
    }

    public void testRamBytesUsed() {
        IndexInputImmutableLongArray array = createTestArray(new long[] { 1L, 2L, 3L });
        assertTrue(array.ramBytesUsed() > 0);
    }

    public void testClose() {
        IndexInputImmutableLongArray array = createTestArray(new long[] { 1L, 2L, 3L });
        array.close(); // Should not throw any exception
    }

    public void testLargeArray() {
        int size = randomIntBetween(10_000, 100_000);
        long[] testData = new long[size];
        for (int i = 0; i < size; i++) {
            testData[i] = randomLong();
        }

        IndexInputImmutableLongArray array = createTestArray(testData);

        assertEquals(size, array.size());
        for (int i = 0; i < 100; i++) {
            int randomIndex = randomIntBetween(0, size - 1);
            assertEquals(testData[randomIndex], array.get(randomIndex));
        }
    }

    private IndexInputImmutableLongArray createTestArray(long[] data) {
        return new IndexInputImmutableLongArray(data.length, new TestRandomAccessInput(data));
    }

    private static class TestRandomAccessInput implements RandomAccessInput {
        private final long[] data;

        TestRandomAccessInput(long[] data) {
            this.data = data;
        }

        @Override
        public byte readByte(long pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public short readShort(long pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readInt(long pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long readLong(long pos) throws IOException {
            if (pos < 0 || pos >= data.length * 8L) {
                throw new IOException("Invalid position: " + pos);
            }
            return data[(int) (pos / 8)];
        }

        @Override
        public long length() {
            return data.length * 8L;
        }
    }
}
