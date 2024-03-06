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
        StatsHolder statsHolder = new StatsHolder(dimensionNames, 10_000);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        populateStats(statsHolder, usedDimensionValues, 100, 10);
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(statsHolder, tierDimensionValue);

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        MultiDimensionCacheStats deserialized = new MultiDimensionCacheStats(is);

        StatsHolderTests.checkStatsHolderEquality(stats.statsHolder, deserialized.statsHolder);
        assertEquals(stats.tierDimensionValue, deserialized.tierDimensionValue);
    }

    public void testAddAndGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames, 10_000);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);

        Map<Set<CacheStatsDimension>, CacheStatsResponse> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(statsHolder, tierDimensionValue);

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
                stats.statsHolder.getDimensionNames(),
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

    public void testEmptyDimsList() throws Exception {
        // If the dimension list is empty, the map should have only one entry, from the empty set -> the total stats.
        StatsHolder statsHolder = new StatsHolder(List.of());
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 100);
        populateStats(statsHolder, usedDimensionValues, 10, 100);
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(statsHolder, tierDimensionValue);

        assertEquals(stats.getTotalStats(), stats.getStatsByDimensions(List.of()));
        assertEquals(stats.getTotalHits(), stats.getHitsByDimensions(List.of()));
        assertEquals(stats.getTotalMisses(), stats.getMissesByDimensions(List.of()));
        assertEquals(stats.getTotalEvictions(), stats.getEvictionsByDimensions(List.of()));
        assertEquals(stats.getTotalMemorySize(), stats.getMemorySizeByDimensions(List.of()));
        assertEquals(stats.getTotalEntries(), stats.getEntriesByDimensions(List.of()));
        assertEquals(1, stats.statsHolder.getMap().size());
    }

    public void testTierLogic() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<Set<CacheStatsDimension>, CacheStatsResponse> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(statsHolder, tierDimensionValue);

        CacheStatsDimension tierDim = new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, tierDimensionValue);
        CacheStatsDimension wrongTierDim = new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, "wrong_value");

        for (int i = 0; i < 1000; i++) {
            List<CacheStatsDimension> aggregationDims = getRandomDimList(
                statsHolder.getDimensionNames(),
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

    static Map<String, List<String>> getUsedDimensionValues(StatsHolder statsHolder, int numValuesPerDim) {
        Map<String, List<String>> usedDimensionValues = new HashMap<>();
        for (int i = 0; i < statsHolder.getDimensionNames().size(); i++) {
            List<String> values = new ArrayList<>();
            for (int j = 0; j < numValuesPerDim; j++) {
                values.add(UUID.randomUUID().toString());
            }
            usedDimensionValues.put(statsHolder.getDimensionNames().get(i), values);
        }
        return usedDimensionValues;
    }

    static Map<Set<CacheStatsDimension>, CacheStatsResponse> populateStats(
        StatsHolder statsHolder,
        Map<String, List<String>> usedDimensionValues,
        int numDistinctValuePairs,
        int numRepetitionsPerValue
    ) {
        Map<Set<CacheStatsDimension>, CacheStatsResponse> expected = new HashMap<>();

        Random rand = Randomness.get();
        for (int i = 0; i < numDistinctValuePairs; i++) {
            List<CacheStatsDimension> dimensions = getRandomDimList(statsHolder.getDimensionNames(), usedDimensionValues, true, rand);
            Set<CacheStatsDimension> dimSet = new HashSet<>(dimensions);
            if (expected.get(dimSet) == null) {
                expected.put(dimSet, new CacheStatsResponse());
            }

            for (int j = 0; j < numRepetitionsPerValue; j++) {

                int numHitIncrements = rand.nextInt(10);
                for (int k = 0; k < numHitIncrements; k++) {
                    statsHolder.incrementHitsByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).hits.inc();
                }

                int numMissIncrements = rand.nextInt(10);
                for (int k = 0; k < numMissIncrements; k++) {
                    statsHolder.incrementMissesByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).misses.inc();
                }

                int numEvictionIncrements = rand.nextInt(10);
                for (int k = 0; k < numEvictionIncrements; k++) {
                    statsHolder.incrementEvictionsByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).evictions.inc();
                }

                int numMemorySizeIncrements = rand.nextInt(10);
                for (int k = 0; k < numMemorySizeIncrements; k++) {
                    long memIncrementAmount = rand.nextInt(5000);
                    statsHolder.incrementMemorySizeByDimensions(dimensions, memIncrementAmount);
                    expected.get(new HashSet<>(dimensions)).memorySize.inc(memIncrementAmount);
                }

                int numEntryIncrements = rand.nextInt(9) + 1;
                for (int k = 0; k < numEntryIncrements; k++) {
                    statsHolder.incrementEntriesByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).entries.inc();
                }

                int numEntryDecrements = rand.nextInt(numEntryIncrements);
                for (int k = 0; k < numEntryDecrements; k++) {
                    statsHolder.decrementEntriesByDimensions(dimensions);
                    expected.get(new HashSet<>(dimensions)).entries.dec();
                }
            }
        }
        return expected;
    }

    private static List<CacheStatsDimension> getRandomDimList(
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
