/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AbstractFuzzySetTests extends OpenSearchTestCase {

    public void testBasicOperations() throws IOException {
        TestFuzzySet testSet = new TestFuzzySet();
        BytesRef testValue = new BytesRef("test");
        testSet.add(testValue);
        assertTrue(testSet.addedValues.contains(testValue));
        assertEquals(FuzzySet.Result.MAYBE, testSet.contains(testValue));
    }

    public void testAddAll() throws IOException {
        TestFuzzySet testSet = new TestFuzzySet();
        List<BytesRef> values = List.of(new BytesRef("test1"), new BytesRef("test2"), new BytesRef("test3"));
        testSet.addAll(() -> values.iterator());
        assertEquals(values.size(), testSet.addedValues.size());
        assertTrue(testSet.addedValues.containsAll(values));
    }

    public void testContains() throws IOException {
        TestFuzzySet testSet = new TestFuzzySet();
        BytesRef testValue = new BytesRef("test");
        FuzzySet.Result result = testSet.contains(testValue);
        assertNotNull(result);
    }

    public void testGenerateKey() throws IOException {
        TestFuzzySet testSet = new TestFuzzySet();
        BytesRef value = new BytesRef("test");
        long key1 = testSet.generateKey(value);
        long key2 = testSet.generateKey(value);
        assertEquals(key1, key2);
        BytesRef differentValue = new BytesRef("different");
        assertNotEquals(key1, testSet.generateKey(differentValue));
    }

    public void testAssertAllElementsExist() throws IOException {
        TestFuzzySet testSet = new TestFuzzySet();
        List<BytesRef> values = List.of(new BytesRef("test1"), new BytesRef("test2"), new BytesRef("test3"));
        testSet.addAll(() -> values.iterator());
        testSet.assertAllElementsExist(() -> values.iterator());
    }

    public void testEmptyIterator() throws IOException {
        TestFuzzySet testSet = new TestFuzzySet();
        testSet.addAll(() -> List.<BytesRef>of().iterator());
        assertEquals(0, testSet.addedValues.size());
    }

    public void testBloomFilterFunctionality() throws IOException {
        AbstractFuzzySet bloomFilter = new AbstractFuzzySet() {
            private final long[] bitSet = new long[1024];

            @Override
            protected void add(BytesRef value) {
                long hash = generateKey(value);
                int bitIndex = Math.abs((int) (hash % (bitSet.length * 64)));
                bitSet[bitIndex / 64] |= (1L << (bitIndex % 64));
            }

            @Override
            protected Result containsHash(long hash) {
                int bitIndex = Math.abs((int) (hash % (bitSet.length * 64)));
                boolean isSet = (bitSet[bitIndex / 64] & (1L << (bitIndex % 64))) != 0;
                return isSet ? Result.MAYBE : Result.NO;
            }

            @Override
            public SetType setType() {
                return SetType.BLOOM_FILTER_V1;
            }

            @Override
            public boolean isSaturated() {
                return false;
            }

            @Override
            public void writeTo(DataOutput out) throws IOException {}

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public void close() throws IOException {}
        };

        List<BytesRef> addedElements = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            BytesRef value = new BytesRef("test" + i);
            addedElements.add(value);
            bloomFilter.add(value);
        }

        for (BytesRef value : addedElements) {
            assertEquals("Added element should return MAYBE", FuzzySet.Result.MAYBE, bloomFilter.contains(value));
        }

        int falsePositives = 0;
        int testCount = 1000;
        for (int i = 0; i < testCount; i++) {
            BytesRef nonExistentValue = new BytesRef("nonexistent" + i);
            if (bloomFilter.contains(nonExistentValue) == FuzzySet.Result.MAYBE) {
                falsePositives++;
            }
        }

        double falsePositiveRate = (double) falsePositives / testCount;
        assertTrue("False positive rate should be reasonable", falsePositiveRate < 0.2);

        BytesRef testValue = new BytesRef("testValue");
        long hash1 = bloomFilter.generateKey(testValue);
        long hash2 = bloomFilter.generateKey(testValue);
        assertEquals("Hash should be consistent for same input", hash1, hash2);
    }

    private static class TestFuzzySet extends AbstractFuzzySet {
        List<BytesRef> addedValues = new ArrayList<>();

        @Override
        protected void add(BytesRef value) {
            addedValues.add(value);
        }

        @Override
        protected Result containsHash(long hash) {
            return Result.MAYBE;
        }

        @Override
        public SetType setType() {
            return SetType.BLOOM_FILTER_V1;
        }

        @Override
        public boolean isSaturated() {
            return false;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            // No-op for test
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() throws IOException {
            // No-op for test
        }
    }
}
