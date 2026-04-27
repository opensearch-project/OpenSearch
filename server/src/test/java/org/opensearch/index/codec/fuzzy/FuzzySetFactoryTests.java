/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FuzzySetFactoryTests extends OpenSearchTestCase {

    private static final String TEST_FIELD = "test_field";

    public void testCreateBloomFilter() throws IOException {
        Map<String, FuzzySetParameters> setTypeMap = new HashMap<>();
        setTypeMap.put(TEST_FIELD, new FuzzySetParameters(() -> 0.1));
        FuzzySetFactory factory = new FuzzySetFactory(setTypeMap);
        FuzzySet fuzzySet = factory.createFuzzySet(100, TEST_FIELD, () -> createTestIterator());
        assertNotNull(fuzzySet);
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, fuzzySet.setType());
    }

    public void testBloomFilterFunctionality() throws IOException {
        Map<String, FuzzySetParameters> setTypeMap = new HashMap<>();
        setTypeMap.put(TEST_FIELD, new FuzzySetParameters(() -> 0.1));
        FuzzySetFactory factory = new FuzzySetFactory(setTypeMap);
        int maxDocs = 100;
        FuzzySet fuzzySet = factory.createFuzzySet(maxDocs, TEST_FIELD, () -> {
            return Arrays.asList(new BytesRef("test1"), new BytesRef("test2"), new BytesRef("test3")).iterator();
        });
        assertNotNull("Bloom filter should be created", fuzzySet);
        assertEquals("Should be bloom filter type", FuzzySet.SetType.BLOOM_FILTER_V1, fuzzySet.setType());
        assertEquals("Should return MAYBE for existing element", FuzzySet.Result.MAYBE, fuzzySet.contains(new BytesRef("test1")));
        assertEquals("Should return MAYBE for existing element", FuzzySet.Result.MAYBE, fuzzySet.contains(new BytesRef("test2")));
        assertEquals("Should return MAYBE for existing element", FuzzySet.Result.MAYBE, fuzzySet.contains(new BytesRef("test3")));
        FuzzySet.Result result = fuzzySet.contains(new BytesRef("definitely_not_in_set_xyz_123"));
        assertTrue("Should return NO or MAYBE for non-existent element", result == FuzzySet.Result.NO || result == FuzzySet.Result.MAYBE);

        if (result == FuzzySet.Result.MAYBE) {
            logger.info("Got false positive for non-existent element (expected behavior with small probability)");
        }
    }

    public void testBloomFilterWithDifferentSizes() throws IOException {
        Map<String, FuzzySetParameters> setTypeMap = new HashMap<>();
        setTypeMap.put(TEST_FIELD, new FuzzySetParameters(() -> 0.01));
        FuzzySetFactory factory = new FuzzySetFactory(setTypeMap);
        int[] sizes = { 100, 1000, 10000 };
        for (int size : sizes) {
            FuzzySet fuzzySet = factory.createFuzzySet(size * 2, TEST_FIELD, () -> {
                List<BytesRef> elements = new ArrayList<>();
                for (int i = 0; i < size / 2; i++) {
                    elements.add(new BytesRef("test" + i));
                }
                return elements.iterator();
            });
            for (int i = 0; i < size / 2; i++) {
                assertEquals(
                    "Should return MAYBE for existing element in size " + size,
                    FuzzySet.Result.MAYBE,
                    fuzzySet.contains(new BytesRef("test" + i))
                );
            }
            int falsePositives = 0;
            int testCount = 100;
            for (int i = 0; i < testCount; i++) {
                FuzzySet.Result result = fuzzySet.contains(new BytesRef("nonexistent_" + i));
                if (result == FuzzySet.Result.MAYBE) {
                    falsePositives++;
                }
            }
            double falsePositiveRate = (double) falsePositives / testCount;
            assertTrue(
                "False positive rate " + falsePositiveRate + " should be below threshold for size " + size,
                falsePositiveRate <= 0.04
            );
        }
    }

    public void testBloomFilterWithVerySmallSet() throws IOException {
        Map<String, FuzzySetParameters> setTypeMap = new HashMap<>();
        setTypeMap.put(TEST_FIELD, new FuzzySetParameters(() -> 0.01));
        FuzzySetFactory factory = new FuzzySetFactory(setTypeMap);
        int size = 10;
        FuzzySet fuzzySet = factory.createFuzzySet(100, TEST_FIELD, () -> {
            List<BytesRef> elements = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                elements.add(new BytesRef("smalltest" + i));
            }
            return elements.iterator();
        });
        for (int i = 0; i < size; i++) {
            assertEquals(
                "Should return MAYBE for existing element",
                FuzzySet.Result.MAYBE,
                fuzzySet.contains(new BytesRef("smalltest" + i))
            );
        }
        int falsePositives = 0;
        int testCount = 100;
        for (int i = 0; i < testCount; i++) {
            if (fuzzySet.contains(new BytesRef("distinct_nonexistent_" + i)) == FuzzySet.Result.MAYBE) {
                falsePositives++;
            }
        }
        double falsePositiveRate = (double) falsePositives / testCount;
        assertTrue("False positive rate " + falsePositiveRate + " should be reasonable for small set", falsePositiveRate <= 0.02);
    }

    public void testBloomFilterFalsePositiveRate() throws IOException {
        double targetFpp = 0.1;
        Map<String, FuzzySetParameters> setTypeMap = new HashMap<>();
        setTypeMap.put(TEST_FIELD, new FuzzySetParameters(() -> targetFpp));
        FuzzySetFactory factory = new FuzzySetFactory(setTypeMap);
        int maxDocs = 1000;
        FuzzySet fuzzySet = factory.createFuzzySet(maxDocs, TEST_FIELD, () -> {
            List<BytesRef> elements = new ArrayList<>();
            for (int i = 0; i < maxDocs; i++) {
                elements.add(new BytesRef("test" + i));
            }
            return elements.iterator();
        });
        int falsePositives = 0;
        int testCount = 1000;
        for (int i = 0; i < testCount; i++) {
            if (fuzzySet.contains(new BytesRef("nonexistent" + i)) == FuzzySet.Result.MAYBE) {
                falsePositives++;
            }
        }
        double actualFpp = (double) falsePositives / testCount;
        assertTrue("False positive rate " + actualFpp + " should be close to target " + targetFpp, actualFpp <= targetFpp * 1.5);
    }

    public void testCreateWithInvalidField() {
        Map<String, FuzzySetParameters> setTypeMap = new HashMap<>();
        setTypeMap.put(TEST_FIELD, new FuzzySetParameters(() -> 0.1));
        FuzzySetFactory factory = new FuzzySetFactory(setTypeMap);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> factory.createFuzzySet(100, "non_existent_field", () -> createTestIterator())
        );
        assertTrue(exception.getMessage().contains("non_existent_field"));
    }

    public void testDeserializeFuzzySet() throws IOException {
        BloomFilter originalFilter = new BloomFilter(100, 0.1, () -> createTestIterator());
        byte[] serialized = serializeFilter(originalFilter);
        FuzzySet deserializedSet = FuzzySetFactory.deserializeFuzzySet(createTestIndexInput(serialized));
        assertNotNull(deserializedSet);
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, deserializedSet.setType());
    }

    public void testMultipleFields() throws IOException {
        Map<String, FuzzySetParameters> setTypeMap = new HashMap<>();
        setTypeMap.put("field1", new FuzzySetParameters(() -> 0.1));
        setTypeMap.put("field2", new FuzzySetParameters(() -> 0.2));
        FuzzySetFactory factory = new FuzzySetFactory(setTypeMap);
        FuzzySet set1 = factory.createFuzzySet(100, "field1", () -> createTestIterator());
        FuzzySet set2 = factory.createFuzzySet(100, "field2", () -> createTestIterator());
        assertNotNull(set1);
        assertNotNull(set2);
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, set1.setType());
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, set2.setType());
    }

    private Iterator<BytesRef> createTestIterator() {
        return new Iterator<BytesRef>() {
            private int count = 3;

            @Override
            public boolean hasNext() {
                return count > 0;
            }

            @Override
            public BytesRef next() {
                count--;
                return new BytesRef("test" + count);
            }
        };
    }

    private Iterator<BytesRef> createRandomIterator(int size) {
        return new Iterator<BytesRef>() {
            private int remaining = size;

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public BytesRef next() {
                remaining--;
                return new BytesRef(randomAlphaOfLength(10));
            }
        };
    }

    private byte[] serializeFilter(BloomFilter filter) throws IOException {
        byte[] buffer = new byte[8192];
        ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        out.writeString(filter.setType().getSetName());
        filter.writeTo(out);
        return buffer;
    }

    private IndexInput createTestIndexInput(byte[] data) {
        return new IndexInput("test") {
            private long pos = 0;

            @Override
            public void close() {}

            @Override
            public long getFilePointer() {
                return pos;
            }

            @Override
            public void seek(long pos) {
                this.pos = pos;
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public IndexInput slice(String sliceDescription, long offset, long length) {
                return null;
            }

            @Override
            public byte readByte() {
                return data[(int) pos++];
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) {
                System.arraycopy(data, (int) pos, b, offset, len);
                pos += len;
            }

            @Override
            public RandomAccessInput randomAccessSlice(long offset, long length) {
                return new TestRandomAccessInput(data, offset);
            }

            @Override
            public String readString() throws IOException {
                // Read VInt-encoded length (simplified: assuming small strings for tests)
                int length = readByte() & 0xFF;
                if (length > 127) {
                    throw new UnsupportedOperationException(
                        "VInt decoding not fully implemented in test mock - " + "string length exceeds 127 characters"
                    );
                }
                byte[] bytes = new byte[length];
                readBytes(bytes, 0, length);
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };
    }

    private static class TestRandomAccessInput implements RandomAccessInput {
        private final byte[] data;
        private final long offset;

        TestRandomAccessInput(byte[] data, long offset) {
            this.data = data;
            this.offset = offset;
        }

        @Override
        public byte readByte(long pos) {
            return data[(int) (offset + pos)];
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
        public long readLong(long pos) {
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (data[(int) (offset + pos + i)] & 0xFF);
            }
            return value;
        }

        @Override
        public long length() {
            return data.length - offset;
        }
    }
}
