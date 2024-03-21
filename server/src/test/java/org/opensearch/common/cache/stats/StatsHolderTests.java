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
import java.util.Set;

import static org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests.getUsedDimensionValues;
import static org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests.populateStats;

public class StatsHolderTests extends OpenSearchTestCase {
    // Since StatsHolder does not expose getter methods for aggregating stats,
    // we test the incrementing functionality in combination with MultiDimensionCacheStats,
    // in MultiDimensionCacheStatsTests.java.

    public void testKeyEquality() throws Exception {
        List<String> dims1 = List.of("1", "2", "3");
        StatsHolder.Key key1 = new StatsHolder.Key(dims1);

        List<String> dims2 = List.of("1", "2", "3");
        StatsHolder.Key key2 = new StatsHolder.Key(dims2);

        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    public void testReset() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<Set<CacheStatsDimension>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 100, 10);

        statsHolder.reset();

        for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
            CacheStatsCounter originalCounter = expected.get(dimSet);
            originalCounter.sizeInBytes = new CounterMetric();
            originalCounter.entries = new CounterMetric();

            StatsHolder.Key key = new StatsHolder.Key(StatsHolder.getOrderedDimensionValues(new ArrayList<>(dimSet), dimensionNames));
            CacheStatsCounter actual = statsHolder.getStatsMap().get(key);
            assertEquals(originalCounter, actual);
        }

        CacheStatsCounter expectedTotal = new CacheStatsCounter();
        for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
            expectedTotal.add(expected.get(dimSet));
        }
        expectedTotal.sizeInBytes = new CounterMetric();
        expectedTotal.entries = new CounterMetric();
    }

    public void testDropStatsForDimensions() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        populateStats(statsHolder, usedDimensionValues, 100, 10);

        List<CacheStatsDimension> dims = getRandomUsedDimensions(usedDimensionValues);
        int originalSize = statsHolder.getStatsMap().size();
        StatsHolder.Key key = new StatsHolder.Key(StatsHolder.getOrderedDimensionValues(dims, dimensionNames));
        assertNotNull(statsHolder.getStatsMap().get(key));

        statsHolder.dropStatsForDimensions(dims);
        assertNull(statsHolder.getStatsMap().get(key));
        assertEquals(originalSize - 1, statsHolder.getStatsMap().size());
    }

    private List<CacheStatsDimension> getRandomUsedDimensions(Map<String, List<String>> usedDimensionValues) {
        Random rand = Randomness.get();
        List<CacheStatsDimension> result = new ArrayList<>();
        for (String dimName : usedDimensionValues.keySet()) {
            List<String> dimValues = usedDimensionValues.get(dimName);
            String dimValue = dimValues.get(rand.nextInt(dimValues.size()));
            result.add(new CacheStatsDimension(dimName, dimValue));
        }
        return result;
    }
}
