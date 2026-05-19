/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class BloomFilterTests extends OpenSearchTestCase {

    public void testBloomFilterSerializationDeserialization() throws IOException {
        int elementCount = randomIntBetween(1, 100);
        long maxDocs = elementCount * 10L; // Keeping this high so that it ensures some bits are not set.
        BloomFilter filter = new BloomFilter(maxDocs, getFpp(), () -> idIterator(elementCount));
        byte[] buffer = new byte[(int) maxDocs * 5];
        ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        out.writeString(filter.setType().getSetName());
        filter.writeTo(out);
        FuzzySet reconstructedFilter = FuzzySetFactory.deserializeFuzzySet(new ByteArrayIndexInput("filter", buffer));
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, reconstructedFilter.setType());
        Iterator<BytesRef> idIterator = idIterator(elementCount);
        while (idIterator.hasNext()) {
            BytesRef element = idIterator.next();
            assertEquals(FuzzySet.Result.MAYBE, reconstructedFilter.contains(element));
            assertEquals(FuzzySet.Result.MAYBE, filter.contains(element));
        }
    }

    public void testBloomFilterIsSaturated_returnsTrue() throws IOException {
        BloomFilter bloomFilter = new BloomFilter(1L, getFpp(), () -> idIterator(1000));
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, bloomFilter.setType());
        assertEquals(true, bloomFilter.isSaturated());
    }

    public void testBloomFilterEnabled() throws IOException {
        int elementCount = 1000;
        double fpp = 0.01;
        List<String> addedElements = new ArrayList<>();
        for (int i = 0; i < elementCount; i++) {
            addedElements.add("test" + i);
        }

        BloomFilter filter = new BloomFilter(elementCount, fpp, () -> addedElements.stream().map(BytesRef::new).iterator());

        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, filter.setType());
        for (String element : addedElements) {
            assertEquals("Should find added element: " + element, FuzzySet.Result.MAYBE, filter.contains(new BytesRef(element)));
        }

        logger.info("All {} elements correctly identified in bloom filter", elementCount);
    }

    public void testBloomFilterEffectiveness() throws IOException {
        int elementCount = 10000;
        double targetFpp = 0.01;
        Set<String> addedElements = new HashSet<>();
        for (int i = 0; i < elementCount; i++) {
            addedElements.add("element" + randomAlphaOfLength(10) + i);
        }

        BloomFilter filter = new BloomFilter(elementCount, targetFpp, () -> addedElements.stream().map(BytesRef::new).iterator());
        int testCount = 20000;
        int falsePositives = 0;
        Set<String> testElements = new HashSet<>();
        for (int i = 0; i < testCount; i++) {
            String testElement;
            do {
                testElement = "test" + randomAlphaOfLength(10) + i;
            } while (addedElements.contains(testElement));
            testElements.add(testElement);
        }
        for (String element : testElements) {
            if (filter.contains(new BytesRef(element)) == FuzzySet.Result.MAYBE) {
                falsePositives++;
            }
        }

        double actualFpp = (double) falsePositives / testCount;
        logger.info("Bloom filter stats - Target FPP: {}, Actual FPP: {}, Elements: {}", targetFpp, actualFpp, elementCount);

        assertTrue("False positive rate should be reasonable", actualFpp <= targetFpp * 2);
    }

    public void testBloomFilterDistribution() throws IOException {
        int elementCount = 5000;
        double fpp = 0.01;
        long expectedBits = (long) Math.ceil(-elementCount * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        List<String> elements = new ArrayList<>();
        for (int i = 0; i < elementCount; i++) {
            elements.add("prefix" + randomAlphaOfLength(5) + i);
        }

        BloomFilter filter = new BloomFilter(elementCount, fpp, () -> elements.stream().map(BytesRef::new).iterator());
        long setBits = 0;
        for (long i = 0; i < expectedBits; i++) {
            if (filter.containsHash(i) == FuzzySet.Result.MAYBE) {
                setBits++;
            }
        }

        double fillRatio = (double) setBits / expectedBits;
        logger.info("Bloom filter fill ratio: {}", fillRatio);
        assertTrue("Fill ratio should be reasonable (between 0.1 and 0.9)", fillRatio > 0.1 && fillRatio < 0.9);
    }

    public void testBloomFilterConcurrency() throws Exception {
        int elementsPerThread = 1000;
        int numThreads = 3;
        double fpp = 0.01;

        Set<String> initialElements = new HashSet<>();
        for (int i = 0; i < elementsPerThread; i++) {
            initialElements.add("initial" + i);
        }

        BloomFilter filter = new BloomFilter(
            elementsPerThread * (numThreads + 1),
            fpp,
            () -> initialElements.stream().map(BytesRef::new).iterator()
        );

        for (String element : initialElements) {
            assertEquals(FuzzySet.Result.MAYBE, filter.contains(new BytesRef(element)));
        }

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(numThreads);
        AtomicInteger falsePositives = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < elementsPerThread; i++) {
                        String element = "thread" + threadId + "element" + i;
                        if (filter.contains(new BytesRef(element)) == FuzzySet.Result.MAYBE && !initialElements.contains(element)) {
                            falsePositives.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    fail("Thread execution failed: " + e.getMessage());
                } finally {
                    endLatch.countDown();
                }
            });
            thread.start();
        }

        startLatch.countDown();
        endLatch.await();

        double fpRate = (double) falsePositives.get() / (elementsPerThread * numThreads);
        logger.info("Concurrent false positive rate: {}", fpRate);
        assertTrue("Concurrent false positive rate should be reasonable", fpRate <= fpp * 2);
    }

    public void testBloomFilterIsSaturated_returnsFalse() throws IOException {
        int elementCount = randomIntBetween(1, 100);
        BloomFilter bloomFilter = new BloomFilter(20000, getFpp(), () -> idIterator(elementCount));
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, bloomFilter.setType());
        assertEquals(false, bloomFilter.isSaturated());
    }

    public void testBloomFilterWithLargeCapacity() throws IOException {
        long maxDocs = randomLongBetween(Integer.MAX_VALUE, 5L * Integer.MAX_VALUE);
        BloomFilter bloomFilter = new BloomFilter(maxDocs, getFpp(), () -> List.of(new BytesRef("bar")).iterator());
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, bloomFilter.setType());
    }

    public void testBloomFilterContainsElement() throws IOException {
        int elementCount = 100;
        double fpp = 0.01;
        BloomFilter filter = new BloomFilter(1000, fpp, () -> idIterator(elementCount));
        for (int i = 0; i < elementCount; i++) {
            assertEquals(FuzzySet.Result.MAYBE, filter.contains(new BytesRef(Integer.toString(i))));
        }
        int falsePositives = 0;
        int testCount = 100;
        for (int i = elementCount; i < elementCount + testCount; i++) {
            if (filter.contains(new BytesRef(Integer.toString(i))) == FuzzySet.Result.MAYBE) {
                falsePositives++;
            }
        }
        double actualFpp = (double) falsePositives / testCount;
        logger.info("False positive rate: {}", actualFpp);
        assertTrue("False positive rate should be reasonable", actualFpp <= fpp * 2);
    }

    public void testBloomFilterWithDifferentFPP() throws IOException {
        int elementCount = 1000;
        double[] fpps = { 0.01, 0.05, 0.1, 0.2 };

        for (double fpp : fpps) {
            BloomFilter filter = new BloomFilter(elementCount, fpp, () -> idIterator(elementCount));
            assertFalse(filter.isSaturated());
            Iterator<BytesRef> iterator = idIterator(elementCount);
            while (iterator.hasNext()) {
                assertEquals(FuzzySet.Result.MAYBE, filter.contains(iterator.next()));
            }
        }
    }

    public void testBloomFilterFalsePositiveRate() throws IOException {
        int elementCount = 10000;
        double targetFpp = 0.01;
        BloomFilter filter = new BloomFilter(elementCount, targetFpp, () -> idIterator(elementCount));
        for (int i = 0; i < elementCount; i++) {
            assertEquals(FuzzySet.Result.MAYBE, filter.contains(new BytesRef(Integer.toString(i))));
        }
        int falsePositives = 0;
        int testCount = 10000;
        for (int i = elementCount; i < elementCount + testCount; i++) {
            if (filter.contains(new BytesRef(Integer.toString(i))) == FuzzySet.Result.MAYBE) {
                falsePositives++;
            }
        }

        double actualFpp = (double) falsePositives / testCount;
        assertTrue("False positive rate " + actualFpp + " should be close to target " + targetFpp, actualFpp <= targetFpp * 2);
    }

    public void testBloomFilterSerialization() throws IOException {
        int elementCount = 500;
        BloomFilter originalFilter = new BloomFilter(elementCount * 2, 0.05, () -> idIterator(elementCount));

        byte[] serialized = serializeFilter(originalFilter);
        FuzzySet deserializedFilter = deserializeFilter(serialized);

        assertEquals(originalFilter.setType(), deserializedFilter.setType());
        Iterator<BytesRef> iterator = idIterator(elementCount);
        while (iterator.hasNext()) {
            BytesRef element = iterator.next();
            assertEquals(originalFilter.contains(element), deserializedFilter.contains(element));
        }
    }

    private byte[] serializeFilter(BloomFilter filter) throws IOException {
        int bufferSize = (int) Math.max(8192, filter.ramBytesUsed() * 2);
        byte[] buffer = new byte[bufferSize];
        ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        out.writeString(filter.setType().getSetName());
        filter.writeTo(out);
        return buffer;
    }

    private FuzzySet deserializeFilter(byte[] serialized) throws IOException {
        return FuzzySetFactory.deserializeFuzzySet(new ByteArrayIndexInput("filter", serialized));
    }

    private double getFpp() {
        return randomDoubleBetween(0.01, 0.50, true);
    }

    private Iterator<BytesRef> idIterator(int count) {
        return new Iterator<BytesRef>() {
            int cnt = count;

            @Override
            public boolean hasNext() {
                return cnt-- > 0;
            }

            @Override
            public BytesRef next() {
                return new BytesRef(Integer.toString(cnt));
            }
        };
    }
}
