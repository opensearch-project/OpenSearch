/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.Randomness;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class MultiDimensionCacheStatsTests extends OpenSearchTestCase {
    String tierDimensionValue = "tier";

    public void testSerialization() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(dimensionNames, tierDimensionValue);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(stats, 10);
        populateStats(stats, usedDimensionValues, 100, 10);

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        MultiDimensionCacheStats deserialized = new MultiDimensionCacheStats(is);
        assertEquals(stats.map, deserialized.map);
        assertEquals(stats.totalStats, deserialized.totalStats);
        assertEquals(stats.dimensionNames, deserialized.dimensionNames);
    }

    public void testAddAndGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(dimensionNames, tierDimensionValue);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(stats, 10);

        Map<Set<CacheStatsDimension>, CacheStatsResponse> expected = populateStats(stats, usedDimensionValues, 1000, 10);
        // test gets for each distinct combination of values
        for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
            List<CacheStatsDimension> dims = new ArrayList<>(dimSet);
            CacheStatsResponse expectedResponse = expected.get(dimSet);
            CacheStatsResponse actual = stats.getStatsByDimensions(dims);
            assertEquals(expectedResponse, actual);

            assertEquals(expectedResponse.getHits(), stats.getHitsByDimensions(dims));
            assertEquals(expectedResponse.getMisses(), stats.getMissesByDimensions(dims));
            assertEquals(expectedResponse.getEvictions(), stats.getEvictionsByDimensions(dims));
            assertEquals(expectedResponse.getMemorySize(), stats.getMemorySizeByDimensions(dims));
            assertEquals(expectedResponse.getEntries(), stats.getEntriesByDimensions(dims));
        }

        // test gets for aggregations of values: for example, dim1="a", dim2="b", but dim3 and dim4 can be anything
        // test a random subset of these, there are combinatorially many possibilities
        for (int i = 0; i < 1000; i++) {
            List<CacheStatsDimension> aggregationDims = getRandomDimList(
                stats.dimensionNames,
                usedDimensionValues,
                false,
                Randomness.get()
            );
            CacheStatsResponse expectedResponse = new CacheStatsResponse();
            for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
                if (dimSet.containsAll(aggregationDims)) {
                    // Confirmed via debug we get a reasonable number of matching dimensions with this setup
                    expectedResponse.add(expected.get(dimSet));
                }
            }
            assertEquals(expectedResponse, stats.getStatsByDimensions(aggregationDims));

            assertEquals(expectedResponse.getHits(), stats.getHitsByDimensions(aggregationDims));
            assertEquals(expectedResponse.getMisses(), stats.getMissesByDimensions(aggregationDims));
            assertEquals(expectedResponse.getEvictions(), stats.getEvictionsByDimensions(aggregationDims));
            assertEquals(expectedResponse.getMemorySize(), stats.getMemorySizeByDimensions(aggregationDims));
            assertEquals(expectedResponse.getEntries(), stats.getEntriesByDimensions(aggregationDims));
        }

        // test gets for total

        CacheStatsResponse expectedTotal = new CacheStatsResponse();
        for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
            expectedTotal.add(expected.get(dimSet));
        }
        assertEquals(expectedTotal, stats.getTotalStats());

        assertEquals(expectedTotal.getHits(), stats.getTotalHits());
        assertEquals(expectedTotal.getMisses(), stats.getTotalMisses());
        assertEquals(expectedTotal.getEvictions(), stats.getTotalEvictions());
        assertEquals(expectedTotal.getMemorySize(), stats.getTotalMemorySize());
        assertEquals(expectedTotal.getEntries(), stats.getTotalEntries());
    }

    public void testExceedsCap() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(dimensionNames, tierDimensionValue, 1000);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(stats, 100);

        // Try a few more than MAX_DIMENSION_VALUES times because there can be collisions in the randomly selected dimension values
        assertThrows(RuntimeException.class, () -> populateStats(stats, usedDimensionValues, (int) (stats.maxDimensionValues * 1.1), 10));
    }

    public void testEmptyDimsList() throws Exception {
        // If the dimension list is empty, the map should have only one entry, from the empty set -> the total stats.
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(List.of(), tierDimensionValue);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(stats, 100);
        populateStats(stats, usedDimensionValues, 10, 100);
        assertEquals(stats.totalStats, stats.getStatsByDimensions(List.of()));
        assertEquals(stats.getTotalHits(), stats.getHitsByDimensions(List.of()));
        assertEquals(stats.getTotalMisses(), stats.getMissesByDimensions(List.of()));
        assertEquals(stats.getTotalEvictions(), stats.getEvictionsByDimensions(List.of()));
        assertEquals(stats.getTotalMemorySize(), stats.getMemorySizeByDimensions(List.of()));
        assertEquals(stats.getTotalEntries(), stats.getEntriesByDimensions(List.of()));
        assertEquals(1, stats.map.size());
    }

    public void testTierLogic() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(dimensionNames, tierDimensionValue);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(stats, 10);
        Map<Set<CacheStatsDimension>, CacheStatsResponse> expected = populateStats(stats, usedDimensionValues, 1000, 10);

        CacheStatsDimension tierDim = new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, tierDimensionValue);
        CacheStatsDimension wrongTierDim = new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, "wrong_value");

        for (int i = 0; i < 1000; i++) {
            List<CacheStatsDimension> aggregationDims = getRandomDimList(
                stats.dimensionNames,
                usedDimensionValues,
                false,
                Randomness.get()
            );
            List<CacheStatsDimension> aggDimsWithTier = new ArrayList<>(aggregationDims);
            aggDimsWithTier.add(tierDim);

            List<CacheStatsDimension> aggDimsWithWrongTier = new ArrayList<>(aggregationDims);
            aggDimsWithWrongTier.add(wrongTierDim);
            CacheStatsResponse expectedResponse = new CacheStatsResponse();
            for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
                if (dimSet.containsAll(aggregationDims)) {
                    expectedResponse.add(expected.get(dimSet));
                }
            }
            assertEquals(expectedResponse, stats.getStatsByDimensions(aggregationDims));
            assertEquals(expectedResponse, stats.getStatsByDimensions(aggDimsWithTier));
            assertEquals(new CacheStatsResponse(), stats.getStatsByDimensions(aggDimsWithWrongTier));
        }
        assertEquals(stats.getTotalStats(), stats.getStatsByDimensions(List.of(tierDim)));
        assertEquals(new CacheStatsResponse(), stats.getStatsByDimensions(List.of(wrongTierDim)));
    }

    public void testKeyEquality() throws Exception {
        Set<CacheStatsDimension> dims1 = new HashSet<>();
        dims1.add(new CacheStatsDimension("a", "1"));
        dims1.add(new CacheStatsDimension("b", "2"));
        dims1.add(new CacheStatsDimension("c", "3"));
        MultiDimensionCacheStats.Key key1 = new MultiDimensionCacheStats.Key(dims1);

        List<CacheStatsDimension> dims2 = new ArrayList<>();
        dims2.add(new CacheStatsDimension("c", "3"));
        dims2.add(new CacheStatsDimension("a", "1"));
        dims2.add(new CacheStatsDimension("b", "2"));
        MultiDimensionCacheStats.Key key2 = new MultiDimensionCacheStats.Key(dims2);

        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    public void testReset() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(dimensionNames, tierDimensionValue);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(stats, 10);
        Map<Set<CacheStatsDimension>, CacheStatsResponse> expected = populateStats(stats, usedDimensionValues, 100, 10);

        stats.reset();

        for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
            List<CacheStatsDimension> dims = new ArrayList<>(dimSet);
            CacheStatsResponse originalResponse = expected.get(dimSet);
            originalResponse.memorySize = new CounterMetric();
            originalResponse.entries = new CounterMetric();
            CacheStatsResponse actual = stats.getStatsByDimensions(dims);
            assertEquals(originalResponse, actual);

            assertEquals(originalResponse.getHits(), stats.getHitsByDimensions(dims));
            assertEquals(originalResponse.getMisses(), stats.getMissesByDimensions(dims));
            assertEquals(originalResponse.getEvictions(), stats.getEvictionsByDimensions(dims));
            assertEquals(originalResponse.getMemorySize(), stats.getMemorySizeByDimensions(dims));
            assertEquals(originalResponse.getEntries(), stats.getEntriesByDimensions(dims));
        }

        CacheStatsResponse expectedTotal = new CacheStatsResponse();
        for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
            expectedTotal.add(expected.get(dimSet));
        }
        expectedTotal.memorySize = new CounterMetric();
        expectedTotal.entries = new CounterMetric();
        assertEquals(expectedTotal, stats.getTotalStats());

        assertEquals(expectedTotal.getHits(), stats.getTotalHits());
        assertEquals(expectedTotal.getMisses(), stats.getTotalMisses());
        assertEquals(expectedTotal.getEvictions(), stats.getTotalEvictions());
        assertEquals(expectedTotal.getMemorySize(), stats.getTotalMemorySize());
        assertEquals(expectedTotal.getEntries(), stats.getTotalEntries());
    }

    private Map<String, List<String>> getUsedDimensionValues(MultiDimensionCacheStats stats, int numValuesPerDim) {
        Map<String, List<String>> usedDimensionValues = new HashMap<>();
        for (int i = 0; i < stats.dimensionNames.size(); i++) {
            List<String> values = new ArrayList<>();
            for (int j = 0; j < numValuesPerDim; j++) {
                values.add(UUID.randomUUID().toString());
            }
            usedDimensionValues.put(stats.dimensionNames.get(i), values);
        }
        return usedDimensionValues;
    }

    private Map<Set<CacheStatsDimension>, CacheStatsResponse> populateStats(
        MultiDimensionCacheStats stats,
        Map<String, List<String>> usedDimensionValues,
        int numDistinctValuePairs,
        int numRepetitionsPerValue
    ) {
        Map<Set<CacheStatsDimension>, CacheStatsResponse> expected = new HashMap<>();

        Random rand = Randomness.get();
        for (int i = 0; i < numDistinctValuePairs; i++) {
            List<CacheStatsDimension> dimensions = getRandomDimList(stats.dimensionNames, usedDimensionValues, true, rand);
            Set<CacheStatsDimension> dimSet = new HashSet<>(dimensions);
            if (expected.get(dimSet) == null) {
                expected.put(dimSet, new CacheStatsResponse());
            }

            for (int j = 0; j < numRepetitionsPerValue; j++) {

                int numHitIncrements = rand.nextInt(10);
                for (int k = 0; k < numHitIncrements; k++) {
                    stats.incrementHitsByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).hits.inc();
                }

                int numMissIncrements = rand.nextInt(10);
                for (int k = 0; k < numMissIncrements; k++) {
                    stats.incrementMissesByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).misses.inc();
                }

                int numEvictionIncrements = rand.nextInt(10);
                for (int k = 0; k < numEvictionIncrements; k++) {
                    stats.incrementEvictionsByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).evictions.inc();
                }

                int numMemorySizeIncrements = rand.nextInt(10);
                for (int k = 0; k < numMemorySizeIncrements; k++) {
                    long memIncrementAmount = rand.nextInt(5000);
                    stats.incrementMemorySizeByDimensions(dimensions, memIncrementAmount);
                    expected.get(new HashSet<>(dimensions)).memorySize.inc(memIncrementAmount);
                }

                int numEntryIncrements = rand.nextInt(9) + 1;
                for (int k = 0; k < numEntryIncrements; k++) {
                    stats.incrementEntriesByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).entries.inc();
                }

                int numEntryDecrements = rand.nextInt(numEntryIncrements);
                for (int k = 0; k < numEntryDecrements; k++) {
                    stats.decrementEntriesByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).entries.dec();
                }
            }
        }
        return expected;
    }

    private List<CacheStatsDimension> getRandomDimList(
        List<String> dimensionNames,
        Map<String, List<String>> usedDimensionValues,
        boolean pickValueForAllDims,
        Random rand
    ) {
        List<CacheStatsDimension> result = new ArrayList<>();
        for (String dimName : dimensionNames) {
            if (pickValueForAllDims || rand.nextBoolean()) { // if pickValueForAllDims, always pick a value for each dimension, otherwise do
                // so 50% of the time
                int index = between(0, usedDimensionValues.get(dimName).size() - 1);
                result.add(new CacheStatsDimension(dimName, usedDimensionValues.get(dimName).get(index)));
            }
        }
        return result;
    }
}
