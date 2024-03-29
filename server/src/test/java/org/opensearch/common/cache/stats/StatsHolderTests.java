/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.Randomness;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests.getUsedDimensionValues;
import static org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests.populateStats;

public class StatsHolderTests extends OpenSearchTestCase {
    // Since StatsHolder does not expose getter methods for aggregating stats,
    // we test the incrementing functionality in combination with MultiDimensionCacheStats,
    // in MultiDimensionCacheStatsTests.java.

    public void testKeyEquality() throws Exception {
        List<CacheStatsDimension> dims1 = List.of(
            new CacheStatsDimension("A", "1"),
            new CacheStatsDimension("B", "2"),
            new CacheStatsDimension("C", "3")
        );
        StatsHolder.Key key1 = new StatsHolder.Key(dims1);

        List<CacheStatsDimension> dims2 = List.of(
            new CacheStatsDimension("A", "1"),
            new CacheStatsDimension("B", "2"),
            new CacheStatsDimension("C", "3")
        );
        StatsHolder.Key key2 = new StatsHolder.Key(dims2);

        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    public void testReset() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<CacheStatsDimension>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 100, 10);

        statsHolder.reset();

        for (List<CacheStatsDimension> dims : expected.keySet()) {
            CacheStatsCounter originalCounter = expected.get(dims);
            originalCounter.sizeInBytes = new CounterMetric();
            originalCounter.entries = new CounterMetric();

            StatsHolder.Key key = new StatsHolder.Key(StatsHolder.getOrderedDimensions(dims, dimensionNames));
            CacheStatsCounter actual = statsHolder.getStatsMap().get(key);
            assertEquals(originalCounter, actual);
        }

        CacheStatsCounter expectedTotal = new CacheStatsCounter();
        for (List<CacheStatsDimension> dims : expected.keySet()) {
            expectedTotal.add(expected.get(dims));
        }
        expectedTotal.sizeInBytes = new CounterMetric();
        expectedTotal.entries = new CounterMetric();
    }

    public void testKeyContainsAllDimensions() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);

        List<CacheStatsDimension> dims = List.of(new CacheStatsDimension("dim1", "A"), new CacheStatsDimension("dim2", "B"));

        StatsHolder.Key matchingKey = new StatsHolder.Key(
            List.of(new CacheStatsDimension("dim1", "A"), new CacheStatsDimension("dim2", "B"), new CacheStatsDimension("dim3", "C"))
        );
        StatsHolder.Key nonMatchingKey = new StatsHolder.Key(
            List.of(new CacheStatsDimension("dim1", "A"), new CacheStatsDimension("dim2", "Z"), new CacheStatsDimension("dim3", "C"))
        );

        assertTrue(statsHolder.keyContainsAllDimensions(matchingKey, dims));
        assertFalse(statsHolder.keyContainsAllDimensions(nonMatchingKey, dims));

        List<CacheStatsDimension> emptyDims = List.of();
        assertTrue(statsHolder.keyContainsAllDimensions(matchingKey, emptyDims));
        assertTrue(statsHolder.keyContainsAllDimensions(nonMatchingKey, emptyDims));

        List<CacheStatsDimension> illegalDims = List.of(new CacheStatsDimension("invalid_dim", "A"));
        assertThrows(IllegalArgumentException.class, () -> statsHolder.keyContainsAllDimensions(matchingKey, illegalDims));
    }

    public void testDropStatsForDimensions() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        populateStats(statsHolder, usedDimensionValues, 100, 10);

        List<CacheStatsDimension> dimsToRemove = getRandomUsedDimensions(usedDimensionValues);
        int originalSize = statsHolder.getStatsMap().size();

        int numKeysMatchingDimensions = 0;
        for (StatsHolder.Key key : statsHolder.getStatsMap().keySet()) {
            if (statsHolder.keyContainsAllDimensions(key, dimsToRemove)) {
                numKeysMatchingDimensions++;
            }
        }

        statsHolder.removeDimensions(dimsToRemove);
        for (StatsHolder.Key key : statsHolder.getStatsMap().keySet()) {
            assertFalse(statsHolder.keyContainsAllDimensions(key, dimsToRemove));
        }
        assertEquals(originalSize - numKeysMatchingDimensions, statsHolder.getStatsMap().size());
    }

    private List<CacheStatsDimension> getRandomUsedDimensions(Map<String, List<String>> usedDimensionValues) {
        Random rand = Randomness.get();
        List<CacheStatsDimension> result = new ArrayList<>();
        for (String dimName : usedDimensionValues.keySet()) {
            if (rand.nextBoolean()) {
                List<String> dimValues = usedDimensionValues.get(dimName);
                String dimValue = dimValues.get(rand.nextInt(dimValues.size()));
                result.add(new CacheStatsDimension(dimName, dimValue));
            }
        }
        return result;
    }
}
