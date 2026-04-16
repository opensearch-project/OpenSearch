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

public class FuzzySetParametersTests extends OpenSearchTestCase {

    public void testDefaultConstructor() {
        double expectedFpp = 0.3;
        FuzzySetParameters params = new FuzzySetParameters(() -> expectedFpp);
        assertEquals(expectedFpp, params.getFalsePositiveProbability(), 0.0);
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, params.getSetType());
    }

    public void testWithDefaultFPP() {
        FuzzySetParameters params = new FuzzySetParameters(() -> FuzzySetParameters.DEFAULT_FALSE_POSITIVE_PROBABILITY);
        assertEquals(FuzzySetParameters.DEFAULT_FALSE_POSITIVE_PROBABILITY, params.getFalsePositiveProbability(), 0.0);
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, params.getSetType());
    }

    public void testWithRandomFPP() {
        double randomFpp = randomDoubleBetween(0.0, 1.0, true);
        FuzzySetParameters params = new FuzzySetParameters(() -> randomFpp);
        assertEquals(randomFpp, params.getFalsePositiveProbability(), 0.0);
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, params.getSetType());
    }

    public void testSupplierBehavior() {
        double[] values = { 0.1, 0.2, 0.3 };
        int[] index = { 0 };
        FuzzySetParameters params = new FuzzySetParameters(() -> values[index[0]++]);
        assertEquals(0.1, params.getFalsePositiveProbability(), 0.0);
        assertEquals(0.2, params.getFalsePositiveProbability(), 0.0);
        assertEquals(0.3, params.getFalsePositiveProbability(), 0.0);
    }

    public void testConstantFPPSupplier() {
        double constantFpp = randomDoubleBetween(0.0, 1.0, true);
        FuzzySetParameters params = new FuzzySetParameters(() -> constantFpp);
        assertEquals(constantFpp, params.getFalsePositiveProbability(), 0.0);
        assertEquals(constantFpp, params.getFalsePositiveProbability(), 0.0);
        assertEquals(constantFpp, params.getFalsePositiveProbability(), 0.0);
    }

    public void testDefaultFPPValue() {
        assertTrue(FuzzySetParameters.DEFAULT_FALSE_POSITIVE_PROBABILITY > 0.0);
        assertTrue(FuzzySetParameters.DEFAULT_FALSE_POSITIVE_PROBABILITY < 1.0);
    }

    public void testBloomFilterEnabled() {
        FuzzySetParameters params = new FuzzySetParameters(() -> 0.1);
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, params.getSetType());
    }

    public void testBloomFilterFunctionality() throws IOException {
        int expectedItems = 1000;
        int bitSetSize = 8192;
        LongArrayBackedBitSet bloomFilter = new LongArrayBackedBitSet(bitSetSize);
        String[] addedElements = new String[expectedItems];
        for (int i = 0; i < expectedItems; i++) {
            addedElements[i] = "test" + i;
            for (int k = 0; k < 4; k++) {
                long hash = hash(addedElements[i], k);
                bloomFilter.set(Math.abs(hash % bitSetSize));
            }
        }
        for (String element : addedElements) {
            boolean found = true;
            for (int k = 0; k < 4; k++) {
                long hash = hash(element, k);
                if (!bloomFilter.get(Math.abs(hash % bitSetSize))) {
                    found = false;
                    break;
                }
            }
            assertTrue("Should find added element: " + element, found);
        }
        int testCount = 10000;
        int falsePositives = 0;
        for (int i = 0; i < testCount; i++) {
            String nonExistentElement = "nonexistent" + i;
            boolean found = true;
            for (int k = 0; k < 4; k++) {
                long hash = hash(nonExistentElement, k);
                if (!bloomFilter.get(Math.abs(hash % bitSetSize))) {
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
        assertTrue("False positive rate " + falsePositiveRate + " should be reasonable", falsePositiveRate < 0.20);
    }

    public void testBloomFilterSaturation() throws IOException {
        int bitSetSize = 1024;
        LongArrayBackedBitSet bloomFilter = new LongArrayBackedBitSet(bitSetSize);
        int itemsAdded = 0;
        double targetSaturation = 0.7;

        while ((double) bloomFilter.cardinality() / bitSetSize < targetSaturation && itemsAdded < 10000) {
            String item = "item" + itemsAdded;
            for (int k = 0; k < 4; k++) {
                long hash = hash(item, k);
                bloomFilter.set(Math.abs(hash % bitSetSize));
            }
            itemsAdded++;
        }
        double fillRatio = (double) bloomFilter.cardinality() / bitSetSize;
        logger.info("Fill ratio: {}", fillRatio);
        assertTrue("Fill ratio should be significant", fillRatio > 0.3);
        assertTrue("Fill ratio should not be completely saturated", fillRatio < 1.0);
    }

    private long hash(String value, int seed) {
        long h = seed * 31L + value.hashCode();
        h = h * h * h * 31L;
        return h ^ (h >>> 32);
    }
}
