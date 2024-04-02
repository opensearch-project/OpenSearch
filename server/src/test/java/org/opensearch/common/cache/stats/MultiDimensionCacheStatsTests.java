/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class MultiDimensionCacheStatsTests extends OpenSearchTestCase {
    public void testSerialization() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        populateStats(statsHolder, usedDimensionValues, 100, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        MultiDimensionCacheStats deserialized = new MultiDimensionCacheStats(is);

        assertEquals(stats.dimensionNames, deserialized.dimensionNames);
        List<List<String>> pathsInOriginal = new ArrayList<>();
        StatsHolder.traverseStatsTreeHelper(stats.getStatsRoot(), new ArrayList<>(), (node, path) -> pathsInOriginal.add(path));
        for (List<String> path : pathsInOriginal) {
            DimensionNode<CounterSnapshot> originalNode = stats.statsRoot.getNode(path);
            DimensionNode<CounterSnapshot> deserializedNode = deserialized.statsRoot.getNode(path);
            assertNotNull(deserializedNode);
            assertEquals(originalNode.getDimensionValue(), deserializedNode.getDimensionValue());
            assertEquals(originalNode.getStats(), deserializedNode.getStats());
        }
    }

    public void testAddAndGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        // test the value in the map is as expected for each distinct combination of values
        for (List<String> dimensionValues : expected.keySet()) {
            CacheStatsCounter expectedCounter = expected.get(dimensionValues);

            CounterSnapshot actualStatsHolder = statsHolder.getStatsRoot().getNode(dimensionValues).getStats().snapshot();
            CounterSnapshot actualCacheStats = stats.getStatsRoot().getNode(dimensionValues).getStats();

            assertEquals(expectedCounter.snapshot(), actualStatsHolder);
            assertEquals(expectedCounter.snapshot(), actualCacheStats);
        }

        // test gets for total (this also checks sum-of-children logic)
        CacheStatsCounter expectedTotal = new CacheStatsCounter();
        for (List<String> dims : expected.keySet()) {
            expectedTotal.add(expected.get(dims));
        }
        assertEquals(expectedTotal.snapshot(), stats.getTotalStats());

        assertEquals(expectedTotal.getHits(), stats.getTotalHits());
        assertEquals(expectedTotal.getMisses(), stats.getTotalMisses());
        assertEquals(expectedTotal.getEvictions(), stats.getTotalEvictions());
        assertEquals(expectedTotal.getSizeInBytes(), stats.getTotalSizeInBytes());
        assertEquals(expectedTotal.getEntries(), stats.getTotalEntries());
    }

    public void testEmptyDimsList() throws Exception {
        // If the dimension list is empty, the tree should have only the root node containing the total stats.
        StatsHolder statsHolder = new StatsHolder(List.of());
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 100);
        populateStats(statsHolder, usedDimensionValues, 10, 100);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        DimensionNode<CounterSnapshot> statsRoot = stats.getStatsRoot();
        assertEquals(0, statsRoot.children.size());
        assertEquals(stats.getTotalStats(), statsRoot.getStats());
    }

    public void testAggregateByAllDimensions() throws Exception {
        // Aggregating with all dimensions as levels should just give us the same values that were in the original map
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        DimensionNode<CounterSnapshot> aggregated = stats.aggregateByLevels(dimensionNames);
        for (Map.Entry<List<String>, CacheStatsCounter> expectedEntry : expected.entrySet()) {
            List<String> dimensionValues = new ArrayList<>();
            for (String dimValue : expectedEntry.getKey()) {
                dimensionValues.add(dimValue);
            }
            assertEquals(expectedEntry.getValue().snapshot(), aggregated.getNode(dimensionValues).getStats());
        }
    }

    public void testAggregateBySomeDimensions() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        for (int i = 0; i < (1 << dimensionNames.size()); i++) {
            // Test each combination of possible levels
            List<String> levels = new ArrayList<>();
            for (int nameIndex = 0; nameIndex < dimensionNames.size(); nameIndex++) {
                if ((i & (1 << nameIndex)) != 0) {
                    levels.add(dimensionNames.get(nameIndex));
                }
            }
            if (levels.size() == 0) {
                assertThrows(IllegalArgumentException.class, () -> stats.aggregateByLevels(levels));
            } else {
                DimensionNode<CounterSnapshot> aggregated = stats.aggregateByLevels(levels);
                Map<List<String>, DimensionNode<CounterSnapshot>> aggregatedLeafNodes = getAllLeafNodes(aggregated);

                for (Map.Entry<List<String>, DimensionNode<CounterSnapshot>> aggEntry : aggregatedLeafNodes.entrySet()) {
                    CacheStatsCounter expectedCounter = new CacheStatsCounter();
                    for (List<String> expectedDims : expected.keySet()) {
                        if (expectedDims.containsAll(aggEntry.getKey())) {
                            expectedCounter.add(expected.get(expectedDims));
                        }
                    }
                    assertEquals(expectedCounter.snapshot(), aggEntry.getValue().getStats());
                }
            }
        }
    }

    // Get a map from the list of dimension values to the corresponding leaf node.
    private Map<List<String>, DimensionNode<CounterSnapshot>> getAllLeafNodes(DimensionNode<CounterSnapshot> root) {
        Map<List<String>, DimensionNode<CounterSnapshot>> result = new HashMap<>();
        getAllLeafNodesHelper(result, root, new ArrayList<>());
        return result;
    }

    private void getAllLeafNodesHelper(
        Map<List<String>, DimensionNode<CounterSnapshot>> result,
        DimensionNode<CounterSnapshot> current,
        List<String> pathToCurrent
    ) {
        if (current.children.isEmpty()) {
            result.put(pathToCurrent, current);
        } else {
            for (Map.Entry<String, DimensionNode<CounterSnapshot>> entry : current.children.entrySet()) {
                List<String> newPath = new ArrayList<>(pathToCurrent);
                newPath.add(entry.getKey());
                getAllLeafNodesHelper(result, entry.getValue(), newPath);
            }
        }
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

    static Map<List<String>, CacheStatsCounter> populateStats(
        StatsHolder statsHolder,
        Map<String, List<String>> usedDimensionValues,
        int numDistinctValuePairs,
        int numRepetitionsPerValue
    ) {
        Map<List<String>, CacheStatsCounter> expected = new HashMap<>();

        Random rand = Randomness.get();
        for (int i = 0; i < numDistinctValuePairs; i++) {
            List<String> dimensions = getRandomDimList(statsHolder.getDimensionNames(), usedDimensionValues, true, rand);
            if (expected.get(dimensions) == null) {
                expected.put(dimensions, new CacheStatsCounter());
            }
            ICacheKey<String> dummyKey = getDummyKey(dimensions);

            for (int j = 0; j < numRepetitionsPerValue; j++) {

                int numHitIncrements = rand.nextInt(10);
                for (int k = 0; k < numHitIncrements; k++) {
                    statsHolder.incrementHits(dimensions);
                    expected.get(dimensions).hits.inc();
                }

                int numMissIncrements = rand.nextInt(10);
                for (int k = 0; k < numMissIncrements; k++) {
                    statsHolder.incrementMisses(dimensions);
                    expected.get(dimensions).misses.inc();
                }

                int numEvictionIncrements = rand.nextInt(10);
                for (int k = 0; k < numEvictionIncrements; k++) {
                    statsHolder.incrementEvictions(dimensions);
                    expected.get(dimensions).evictions.inc();
                }

                int numMemorySizeIncrements = rand.nextInt(10);
                for (int k = 0; k < numMemorySizeIncrements; k++) {
                    long memIncrementAmount = rand.nextInt(5000);
                    statsHolder.incrementSizeInBytes(dimensions, memIncrementAmount);
                    expected.get(dimensions).sizeInBytes.inc(memIncrementAmount);
                }

                int numEntryIncrements = rand.nextInt(9) + 1;
                for (int k = 0; k < numEntryIncrements; k++) {
                    statsHolder.incrementEntries(dimensions);
                    expected.get(dimensions).entries.inc();
                }

                int numEntryDecrements = rand.nextInt(numEntryIncrements);
                for (int k = 0; k < numEntryDecrements; k++) {
                    statsHolder.decrementEntries(dimensions);
                    expected.get(dimensions).entries.dec();
                }
            }
        }
        return expected;
    }

    private static ICacheKey<String> getDummyKey(List<String> dims) {
        return new ICacheKey<>(null, dims);
    }

    private static List<String> getRandomDimList(
        List<String> dimensionNames,
        Map<String, List<String>> usedDimensionValues,
        boolean pickValueForAllDims,
        Random rand
    ) {
        List<String> result = new ArrayList<>();
        for (String dimName : dimensionNames) {
            if (pickValueForAllDims || rand.nextBoolean()) { // if pickValueForAllDims, always pick a value for each dimension, otherwise do
                // so 50% of the time
                int index = between(0, usedDimensionValues.get(dimName).size() - 1);
                result.add(usedDimensionValues.get(dimName).get(index));
            }
        }
        return result;
    }
}
