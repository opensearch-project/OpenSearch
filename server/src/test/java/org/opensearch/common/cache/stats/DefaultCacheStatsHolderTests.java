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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class DefaultCacheStatsHolderTests extends OpenSearchTestCase {
    private final String storeName = "dummy_store";

    public void testAddAndGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        DefaultCacheStatsHolder cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(cacheStatsHolder, 10);
        Map<List<String>, CacheStats> expected = DefaultCacheStatsHolderTests.populateStats(
            cacheStatsHolder,
            usedDimensionValues,
            1000,
            10
        );

        // test the value in the map is as expected for each distinct combination of values
        for (List<String> dimensionValues : expected.keySet()) {
            CacheStats expectedCounter = expected.get(dimensionValues);

            ImmutableCacheStats actualStatsHolder = DefaultCacheStatsHolderTests.getNode(dimensionValues, cacheStatsHolder.getStatsRoot())
                .getImmutableStats();
            ImmutableCacheStats actualCacheStats = getNode(dimensionValues, cacheStatsHolder.getStatsRoot()).getImmutableStats();

            assertEquals(expectedCounter.immutableSnapshot(), actualStatsHolder);
            assertEquals(expectedCounter.immutableSnapshot(), actualCacheStats);
        }

        // Check overall total matches
        CacheStats expectedTotal = new CacheStats();
        for (List<String> dims : expected.keySet()) {
            expectedTotal.add(expected.get(dims));
        }
        assertEquals(expectedTotal.immutableSnapshot(), cacheStatsHolder.getStatsRoot().getImmutableStats());

        // Check sum of children stats are correct
        assertSumOfChildrenStats(cacheStatsHolder.getStatsRoot());
    }

    public void testReset() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        DefaultCacheStatsHolder cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(cacheStatsHolder, 10);
        Map<List<String>, CacheStats> expected = populateStats(cacheStatsHolder, usedDimensionValues, 100, 10);

        cacheStatsHolder.reset();

        for (List<String> dimensionValues : expected.keySet()) {
            CacheStats originalCounter = expected.get(dimensionValues);
            originalCounter.sizeInBytes = new CounterMetric();
            originalCounter.items = new CounterMetric();

            DefaultCacheStatsHolder.Node node = getNode(dimensionValues, cacheStatsHolder.getStatsRoot());
            ImmutableCacheStats actual = node.getImmutableStats();
            assertEquals(originalCounter.immutableSnapshot(), actual);
        }
    }

    public void testDropStatsForDimensions() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        DefaultCacheStatsHolder cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);

        // Create stats for the following dimension sets
        List<List<String>> populatedStats = List.of(List.of("A1", "B1"), List.of("A2", "B2"), List.of("A2", "B3"));
        for (List<String> dims : populatedStats) {
            cacheStatsHolder.incrementHits(dims);
        }

        assertEquals(3, cacheStatsHolder.getStatsRoot().getImmutableStats().getHits());

        // When we invalidate A2, B2, we should lose the node for B2, but not B3 or A2.

        cacheStatsHolder.removeDimensions(List.of("A2", "B2"));

        assertEquals(2, cacheStatsHolder.getStatsRoot().getImmutableStats().getHits());
        assertNull(getNode(List.of("A2", "B2"), cacheStatsHolder.getStatsRoot()));
        assertNotNull(getNode(List.of("A2"), cacheStatsHolder.getStatsRoot()));
        assertNotNull(getNode(List.of("A2", "B3"), cacheStatsHolder.getStatsRoot()));

        // When we invalidate A1, B1, we should lose the nodes for B1 and also A1, as it has no more children.

        cacheStatsHolder.removeDimensions(List.of("A1", "B1"));

        assertEquals(1, cacheStatsHolder.getStatsRoot().getImmutableStats().getHits());
        assertNull(getNode(List.of("A1", "B1"), cacheStatsHolder.getStatsRoot()));
        assertNull(getNode(List.of("A1"), cacheStatsHolder.getStatsRoot()));

        // When we invalidate the last node, all nodes should be deleted except the root node

        cacheStatsHolder.removeDimensions(List.of("A2", "B3"));
        assertEquals(0, cacheStatsHolder.getStatsRoot().getImmutableStats().getHits());
        assertEquals(0, cacheStatsHolder.getStatsRoot().children.size());
    }

    public void testCount() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        DefaultCacheStatsHolder cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(cacheStatsHolder, 10);
        Map<List<String>, CacheStats> expected = populateStats(cacheStatsHolder, usedDimensionValues, 100, 10);

        long expectedCount = 0L;
        for (CacheStats counter : expected.values()) {
            expectedCount += counter.getItems();
        }
        assertEquals(expectedCount, cacheStatsHolder.count());
    }

    public void testConcurrentRemoval() throws Exception {
        List<String> dimensionNames = List.of("A", "B");
        DefaultCacheStatsHolder cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);

        // Create stats for the following dimension sets
        List<List<String>> populatedStats = new ArrayList<>();
        int numAValues = 10;
        int numBValues = 2;
        for (int indexA = 0; indexA < numAValues; indexA++) {
            for (int indexB = 0; indexB < numBValues; indexB++) {
                populatedStats.add(List.of("A" + indexA, "B" + indexB));
            }
        }
        for (List<String> dims : populatedStats) {
            cacheStatsHolder.incrementHits(dims);
        }

        // Remove a subset of the dimensions concurrently.
        // Remove both (A0, B0), and (A0, B1), so we expect the intermediate node for A0 to be null afterwards.
        // For all the others, remove only the B0 value. Then we expect the intermediate nodes for A1 through A9 to be present
        // and reflect only the stats for their B1 child.

        Thread[] threads = new Thread[numAValues + 1];
        for (int i = 0; i < numAValues; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> { cacheStatsHolder.removeDimensions(List.of("A" + finalI, "B0")); });
        }
        threads[numAValues] = new Thread(() -> { cacheStatsHolder.removeDimensions(List.of("A0", "B1")); });
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        // intermediate node for A0 should be null
        assertNull(getNode(List.of("A0"), cacheStatsHolder.getStatsRoot()));

        // leaf nodes for all B0 values should be null since they were removed
        for (int indexA = 0; indexA < numAValues; indexA++) {
            assertNull(getNode(List.of("A" + indexA, "B0"), cacheStatsHolder.getStatsRoot()));
        }

        // leaf nodes for all B1 values, except (A0, B1), should not be null as they weren't removed,
        // and the intermediate nodes A1 through A9 shouldn't be null as they have remaining children
        for (int indexA = 1; indexA < numAValues; indexA++) {
            DefaultCacheStatsHolder.Node b1LeafNode = getNode(List.of("A" + indexA, "B1"), cacheStatsHolder.getStatsRoot());
            assertNotNull(b1LeafNode);
            assertEquals(new ImmutableCacheStats(1, 0, 0, 0, 0), b1LeafNode.getImmutableStats());
            DefaultCacheStatsHolder.Node intermediateLevelNode = getNode(List.of("A" + indexA), cacheStatsHolder.getStatsRoot());
            assertNotNull(intermediateLevelNode);
            assertEquals(b1LeafNode.getImmutableStats(), intermediateLevelNode.getImmutableStats());
        }
    }

    /**
     * Returns the node found by following these dimension values down from the root node.
     * Returns null if no such node exists.
     */
    static DefaultCacheStatsHolder.Node getNode(List<String> dimensionValues, DefaultCacheStatsHolder.Node root) {
        DefaultCacheStatsHolder.Node current = root;
        for (String dimensionValue : dimensionValues) {
            current = current.getChildren().get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    static Map<List<String>, CacheStats> populateStats(
        DefaultCacheStatsHolder cacheStatsHolder,
        Map<String, List<String>> usedDimensionValues,
        int numDistinctValuePairs,
        int numRepetitionsPerValue
    ) throws InterruptedException {
        return populateStats(List.of(cacheStatsHolder), usedDimensionValues, numDistinctValuePairs, numRepetitionsPerValue);
    }

    static Map<List<String>, CacheStats> populateStats(
        List<DefaultCacheStatsHolder> cacheStatsHolders,
        Map<String, List<String>> usedDimensionValues,
        int numDistinctValuePairs,
        int numRepetitionsPerValue
    ) throws InterruptedException {
        for (DefaultCacheStatsHolder statsHolder : cacheStatsHolders) {
            assertEquals(cacheStatsHolders.get(0).getDimensionNames(), statsHolder.getDimensionNames());
        }
        Map<List<String>, CacheStats> expected = new ConcurrentHashMap<>();
        Thread[] threads = new Thread[numDistinctValuePairs];
        CountDownLatch countDownLatch = new CountDownLatch(numDistinctValuePairs);
        Random rand = Randomness.get();
        List<List<String>> dimensionsForThreads = new ArrayList<>();
        for (int i = 0; i < numDistinctValuePairs; i++) {
            dimensionsForThreads.add(getRandomDimList(cacheStatsHolders.get(0).getDimensionNames(), usedDimensionValues, true, rand));
            int finalI = i;
            threads[i] = new Thread(() -> {
                Random threadRand = Randomness.get();
                List<String> dimensions = dimensionsForThreads.get(finalI);
                expected.computeIfAbsent(dimensions, (key) -> new CacheStats());
                for (DefaultCacheStatsHolder cacheStatsHolder : cacheStatsHolders) {
                    for (int j = 0; j < numRepetitionsPerValue; j++) {
                        CacheStats statsToInc = new CacheStats(
                            threadRand.nextInt(10),
                            threadRand.nextInt(10),
                            threadRand.nextInt(10),
                            threadRand.nextInt(5000),
                            threadRand.nextInt(10)
                        );
                        expected.get(dimensions).hits.inc(statsToInc.getHits());
                        expected.get(dimensions).misses.inc(statsToInc.getMisses());
                        expected.get(dimensions).evictions.inc(statsToInc.getEvictions());
                        expected.get(dimensions).sizeInBytes.inc(statsToInc.getSizeInBytes());
                        expected.get(dimensions).items.inc(statsToInc.getItems());
                        DefaultCacheStatsHolderTests.populateStatsHolderFromStatsValueMap(cacheStatsHolder, Map.of(dimensions, statsToInc));
                    }
                }
                countDownLatch.countDown();
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }
        countDownLatch.await();
        return expected;
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

    static Map<String, List<String>> getUsedDimensionValues(DefaultCacheStatsHolder cacheStatsHolder, int numValuesPerDim) {
        Map<String, List<String>> usedDimensionValues = new HashMap<>();
        for (int i = 0; i < cacheStatsHolder.getDimensionNames().size(); i++) {
            List<String> values = new ArrayList<>();
            for (int j = 0; j < numValuesPerDim; j++) {
                values.add(UUID.randomUUID().toString());
            }
            usedDimensionValues.put(cacheStatsHolder.getDimensionNames().get(i), values);
        }
        return usedDimensionValues;
    }

    private void assertSumOfChildrenStats(DefaultCacheStatsHolder.Node current) {
        if (!current.children.isEmpty()) {
            CacheStats expectedTotal = new CacheStats();
            for (DefaultCacheStatsHolder.Node child : current.children.values()) {
                expectedTotal.add(child.getImmutableStats());
            }
            assertEquals(expectedTotal.immutableSnapshot(), current.getImmutableStats());
            for (DefaultCacheStatsHolder.Node child : current.children.values()) {
                assertSumOfChildrenStats(child);
            }
        }
    }

    public static void populateStatsHolderFromStatsValueMap(
        DefaultCacheStatsHolder cacheStatsHolder,
        Map<List<String>, CacheStats> statsMap
    ) {
        for (Map.Entry<List<String>, CacheStats> entry : statsMap.entrySet()) {
            CacheStats stats = entry.getValue();
            List<String> dims = entry.getKey();
            for (int i = 0; i < stats.getHits(); i++) {
                cacheStatsHolder.incrementHits(dims);
            }
            for (int i = 0; i < stats.getMisses(); i++) {
                cacheStatsHolder.incrementMisses(dims);
            }
            for (int i = 0; i < stats.getEvictions(); i++) {
                cacheStatsHolder.incrementEvictions(dims);
            }
            cacheStatsHolder.incrementSizeInBytes(dims, stats.getSizeInBytes());
            for (int i = 0; i < stats.getItems(); i++) {
                cacheStatsHolder.incrementItems(dims);
            }
        }
    }
}
