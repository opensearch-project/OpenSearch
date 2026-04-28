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
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class FuzzySetTests extends OpenSearchTestCase {

    public void testSetTypeEnumValues() {
        assertEquals("bloom_filter_v1", FuzzySet.SetType.BLOOM_FILTER_V1.getSetName());
    }

    public void testSetTypeFromValidName() {
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, FuzzySet.SetType.from("bloom_filter_v1"));
    }

    public void testSetTypeFromInvalidName() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> FuzzySet.SetType.from("invalid_filter_type")
        );
        assertEquals("There is no implementation for fuzzy set: invalid_filter_type", exception.getMessage());
    }

    public void testMockFuzzySetImplementation() throws IOException {
        MockFuzzySet mockSet = new MockFuzzySet();
        BytesRef testValue = new BytesRef("test");
        assertEquals(FuzzySet.Result.MAYBE, mockSet.contains(testValue));
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, mockSet.setType());
        assertFalse(mockSet.isSaturated());
        mockSet.close();
        assertTrue(mockSet.isClosed());
    }

    public void testBloomFilterImplementation() throws IOException {
        TestBloomFilterFuzzySet bloomFilter = new TestBloomFilterFuzzySet();
        int numItems = 1000;
        String[] addedItems = new String[numItems];
        for (int i = 0; i < numItems; i++) {
            addedItems[i] = "test" + i;
            bloomFilter.addItem(new BytesRef(addedItems[i]));
        }
        for (String item : addedItems) {
            assertEquals("Should find added item: " + item, FuzzySet.Result.MAYBE, bloomFilter.contains(new BytesRef(item)));
        }
        int testCount = 10000;
        int falsePositives = 0;
        for (int i = 0; i < testCount; i++) {
            String nonExistentItem = "nonexistent" + randomAlphaOfLength(10) + i;
            if (bloomFilter.contains(new BytesRef(nonExistentItem)) == FuzzySet.Result.MAYBE) {
                falsePositives++;
            }
        }

        double falsePositiveRate = (double) falsePositives / testCount;
        logger.info("Bloom filter false positive rate: {}", falsePositiveRate);
        assertTrue("False positive rate should be reasonable", falsePositiveRate < 0.1);
    }

    public void testBloomFilterSaturation() throws IOException {
        TestBloomFilterFuzzySet bloomFilter = new TestBloomFilterFuzzySet();
        int itemsAdded = 0;
        while (!bloomFilter.isSaturated() && itemsAdded < 10000) {
            bloomFilter.addItem(new BytesRef("item" + itemsAdded));
            itemsAdded++;
        }

        assertTrue("Bloom filter should eventually reach saturation", bloomFilter.isSaturated());
        logger.info("Bloom filter saturated after {} items", itemsAdded);
    }

    public void testBloomFilterWithDifferentSizes() throws IOException {
        for (int size : new int[] { 100, 1000, 10000 }) {
            TestBloomFilterFuzzySet bloomFilter = new TestBloomFilterFuzzySet(size);
            for (int i = 0; i < size; i++) {
                bloomFilter.addItem(new BytesRef("item" + i));
            }
            for (int i = 0; i < size; i++) {
                assertEquals(FuzzySet.Result.MAYBE, bloomFilter.contains(new BytesRef("item" + i)));
            }

            assertFalse("Should not be saturated with expected number of items", bloomFilter.isSaturated());
        }
    }

    private static class TestBloomFilterFuzzySet implements FuzzySet {
        private final long[] bitArray;
        private final int numBits;
        private final int numHashFunctions;
        private int setBits;

        TestBloomFilterFuzzySet() {
            this(1000);
        }

        TestBloomFilterFuzzySet(int expectedItems) {
            double falsePositiveProb = 0.01;
            this.numBits = optimalNumOfBits(expectedItems, falsePositiveProb);
            this.numHashFunctions = optimalNumOfHashFunctions(expectedItems, numBits);
            this.bitArray = new long[(numBits + 63) / 64];
            this.setBits = 0;
        }

        void addItem(BytesRef value) throws IOException {
            int[] hashValues = getHashValues(value);
            for (int hash : hashValues) {
                int bit = Math.abs(hash % numBits);
                if (!getBit(bit)) {
                    setBit(bit);
                    setBits++;
                }
            }
        }

        @Override
        public Result contains(BytesRef value) {
            int[] hashValues = getHashValues(value);
            for (int hash : hashValues) {
                int bit = Math.abs(hash % numBits);
                if (!getBit(bit)) {
                    return Result.NO;
                }
            }
            return Result.MAYBE;
        }

        @Override
        public SetType setType() {
            return SetType.BLOOM_FILTER_V1;
        }

        @Override
        public boolean isSaturated() {
            return (double) setBits / numBits > 0.9;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            // Implementation not needed for tests
        }

        @Override
        public long ramBytesUsed() {
            return 8L * bitArray.length;
        }

        @Override
        public void close() throws IOException {
            // No-op for tests
        }

        private boolean getBit(int bit) {
            int wordNum = bit >>> 6;
            long bitmask = 1L << bit;
            return (bitArray[wordNum] & bitmask) != 0;
        }

        private void setBit(int bit) {
            int wordNum = bit >>> 6;
            long bitmask = 1L << bit;
            bitArray[wordNum] |= bitmask;
        }

        private int[] getHashValues(BytesRef value) {
            int[] result = new int[numHashFunctions];
            int hash1 = value.hashCode();
            int hash2 = hash1 >>> 16;
            for (int i = 0; i < numHashFunctions; i++) {
                result[i] = hash1 + i * hash2;
            }
            return result;
        }

        private static int optimalNumOfBits(int n, double p) {
            return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
        }

        private static int optimalNumOfHashFunctions(int n, int m) {
            return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
        }
    }

    public void testSetTypeDeserializer() throws IOException {
        FuzzySet.SetType type = FuzzySet.SetType.BLOOM_FILTER_V1;
        assertNotNull(type.getDeserializer());
        MockIndexInput mockInput = new MockIndexInput();
        FuzzySet deserializedSet = type.getDeserializer().apply(mockInput);
        assertNotNull(deserializedSet);
    }

    public void testResultEnumValues() {
        assertEquals(2, FuzzySet.Result.values().length);
        assertTrue(FuzzySet.Result.valueOf("NO") instanceof FuzzySet.Result);
        assertTrue(FuzzySet.Result.valueOf("MAYBE") instanceof FuzzySet.Result);
    }

    private static class MockFuzzySet implements FuzzySet {
        private boolean closed = false;

        @Override
        public SetType setType() {
            return SetType.BLOOM_FILTER_V1;
        }

        @Override
        public Result contains(BytesRef value) {
            return Result.MAYBE;
        }

        @Override
        public boolean isSaturated() {
            return false;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            // No-op for mock
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    private static class MockIndexInput extends IndexInput {
        protected MockIndexInput() {
            super("MockInput");
        }

        @Override
        public byte readByte() throws IOException {
            return 0;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            // No-op for mock
        }

        @Override
        public void close() throws IOException {
            // No-op for mock
        }

        @Override
        public long getFilePointer() {
            return 0;
        }

        @Override
        public void seek(long pos) throws IOException {}

        @Override
        public long length() {
            return 0;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return null;
        }
    }

    public void testFuzzySetWithLargeData() throws IOException {
        MockFuzzySet mockSet = new MockFuzzySet();
        int numOperations = 10000;
        for (int i = 0; i < numOperations; i++) {
            BytesRef value = new BytesRef("test" + i);
            assertEquals(FuzzySet.Result.MAYBE, mockSet.contains(value));
        }

        assertFalse(mockSet.isSaturated());
    }

    public void testSetTypeProperties() {
        assertFalse("BLOOM_FILTER_V1 should have at least one alias", FuzzySet.SetType.BLOOM_FILTER_V1.getSetName().isEmpty());
        assertNotNull("BLOOM_FILTER_V1 should have a non-null deserializer", FuzzySet.SetType.BLOOM_FILTER_V1.getDeserializer());
        for (FuzzySet.SetType type : FuzzySet.SetType.values()) {
            assertFalse("SetType " + type + " should have a non-empty set name", type.getSetName().isEmpty());
        }
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> FuzzySet.SetType.from("invalid_set_type"));
        assertEquals("There is no implementation for fuzzy set: invalid_set_type", exception.getMessage());
    }

    public void testFuzzySetMemoryUsage() throws IOException {
        MockFuzzySet mockSet = new MockFuzzySet();
        assertEquals(0, mockSet.ramBytesUsed());
    }

    public void testMultipleCloseCalls() throws IOException {
        MockFuzzySet mockSet = new MockFuzzySet();
        mockSet.close();
        assertTrue(mockSet.isClosed());
        mockSet.close();
        assertTrue(mockSet.isClosed());
    }
}
