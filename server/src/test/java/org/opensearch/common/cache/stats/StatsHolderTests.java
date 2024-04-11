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

public class StatsHolderTests extends OpenSearchTestCase {
    public void testAddAndGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = StatsHolderTests.getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = StatsHolderTests.populateStats(statsHolder, usedDimensionValues, 1000, 10);

        // test the value in the map is as expected for each distinct combination of values
        for (List<String> dimensionValues : expected.keySet()) {
            CacheStatsCounter expectedCounter = expected.get(dimensionValues);

            CacheStatsCounterSnapshot actualStatsHolder = StatsHolderTests.getNode(dimensionValues, statsHolder.getStatsRoot())
                .getStatsSnapshot();
            CacheStatsCounterSnapshot actualCacheStats = getNode(dimensionValues, statsHolder.getStatsRoot()).getStatsSnapshot();

            assertEquals(expectedCounter.snapshot(), actualStatsHolder);
            assertEquals(expectedCounter.snapshot(), actualCacheStats);
        }

        // Check overall total matches
        CacheStatsCounter expectedTotal = new CacheStatsCounter();
        for (List<String> dims : expected.keySet()) {
            expectedTotal.add(expected.get(dims));
        }
        assertEquals(expectedTotal.snapshot(), statsHolder.getStatsRoot().getStatsSnapshot());

        // Check sum of children stats are correct
        assertSumOfChildrenStats(statsHolder.getStatsRoot());
    }

    public void testReset() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 100, 10);

        statsHolder.reset();

        for (List<String> dimensionValues : expected.keySet()) {
            CacheStatsCounter originalCounter = expected.get(dimensionValues);
            originalCounter.sizeInBytes = new CounterMetric();
            originalCounter.entries = new CounterMetric();

            DimensionNode node = getNode(dimensionValues, statsHolder.getStatsRoot());
            CacheStatsCounterSnapshot actual = node.getStatsSnapshot();
            assertEquals(originalCounter.snapshot(), actual);
        }
    }

    public void testDropStatsForDimensions() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);

        // Create stats for the following dimension sets
        List<List<String>> populatedStats = List.of(List.of("A1", "B1"), List.of("A2", "B2"), List.of("A2", "B3"));
        for (List<String> dims : populatedStats) {
            statsHolder.incrementHits(dims);
        }

        assertEquals(3, statsHolder.getStatsRoot().getStatsSnapshot().getHits());

        // When we invalidate A2, B2, we should lose the node for B2, but not B3 or A2.

        statsHolder.removeDimensions(List.of("A2", "B2"));

        assertEquals(2, statsHolder.getStatsRoot().getStatsSnapshot().getHits());
        assertNull(getNode(List.of("A2", "B2"), statsHolder.getStatsRoot()));
        assertNotNull(getNode(List.of("A2"), statsHolder.getStatsRoot()));
        assertNotNull(getNode(List.of("A2", "B3"), statsHolder.getStatsRoot()));

        // When we invalidate A1, B1, we should lose the nodes for B1 and also A1, as it has no more children.

        statsHolder.removeDimensions(List.of("A1", "B1"));

        assertEquals(1, statsHolder.getStatsRoot().getStatsSnapshot().getHits());
        assertNull(getNode(List.of("A1", "B1"), statsHolder.getStatsRoot()));
        assertNull(getNode(List.of("A1"), statsHolder.getStatsRoot()));

        // When we invalidate the last node, all nodes should be deleted except the root node

        statsHolder.removeDimensions(List.of("A2", "B3"));
        assertEquals(0, statsHolder.getStatsRoot().getStatsSnapshot().getHits());
        assertEquals(0, statsHolder.getStatsRoot().children.size());
    }

    public void testCount() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 100, 10);

        long expectedCount = 0L;
        for (CacheStatsCounter counter : expected.values()) {
            expectedCount += counter.getEntries();
        }
        assertEquals(expectedCount, statsHolder.count());
    }

    public void testConcurrentRemoval() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);

        // Create stats for the following dimension sets
        List<List<String>> populatedStats = List.of(List.of("A1", "B1"), List.of("A2", "B2"), List.of("A2", "B3"));
        for (List<String> dims : populatedStats) {
            statsHolder.incrementHits(dims);
        }

        // Remove (A2, B2) and (A1, B1), before re-adding (A2, B2). At the end we should have stats for (A2, B2) but not (A1, B1).

        Thread[] threads = new Thread[3];
        CountDownLatch countDownLatch = new CountDownLatch(3);
        threads[0] = new Thread(() -> {
            statsHolder.removeDimensions(List.of("A2", "B2"));
            countDownLatch.countDown();
        });
        threads[1] = new Thread(() -> {
            statsHolder.removeDimensions(List.of("A1", "B1"));
            countDownLatch.countDown();
        });
        threads[2] = new Thread(() -> {
            statsHolder.incrementMisses(List.of("A2", "B2"));
            statsHolder.incrementMisses(List.of("A2", "B3"));
            countDownLatch.countDown();
        });
        for (Thread thread : threads) {
            thread.start();
            // Add short sleep to ensure threads start their functions in order (so that incrementing doesn't happen before removal)
            Thread.sleep(1);
        }
        countDownLatch.await();
        assertNull(getNode(List.of("A1", "B1"), statsHolder.getStatsRoot()));
        assertNull(getNode(List.of("A1"), statsHolder.getStatsRoot()));
        assertNotNull(getNode(List.of("A2", "B2"), statsHolder.getStatsRoot()));
        assertEquals(
            new CacheStatsCounterSnapshot(0, 1, 0, 0, 0),
            getNode(List.of("A2", "B2"), statsHolder.getStatsRoot()).getStatsSnapshot()
        );
        assertEquals(
            new CacheStatsCounterSnapshot(1, 1, 0, 0, 0),
            getNode(List.of("A2", "B3"), statsHolder.getStatsRoot()).getStatsSnapshot()
        );
    }

    /**
     * Returns the node found by following these dimension values down from the root node.
     * Returns null if no such node exists.
     */
    static DimensionNode getNode(List<String> dimensionValues, DimensionNode root) {
        DimensionNode current = root;
        for (String dimensionValue : dimensionValues) {
            current = current.getChildren().get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    static Map<List<String>, CacheStatsCounter> populateStats(
        StatsHolder statsHolder,
        Map<String, List<String>> usedDimensionValues,
        int numDistinctValuePairs,
        int numRepetitionsPerValue
    ) throws InterruptedException {
        Map<List<String>, CacheStatsCounter> expected = new ConcurrentHashMap<>();
        Thread[] threads = new Thread[numDistinctValuePairs];
        CountDownLatch countDownLatch = new CountDownLatch(numDistinctValuePairs);
        Random rand = Randomness.get();
        List<List<String>> dimensionsForThreads = new ArrayList<>();
        for (int i = 0; i < numDistinctValuePairs; i++) {
            dimensionsForThreads.add(getRandomDimList(statsHolder.getDimensionNames(), usedDimensionValues, true, rand));
            int finalI = i;
            threads[i] = new Thread(() -> {
                Random threadRand = Randomness.get();
                List<String> dimensions = dimensionsForThreads.get(finalI);
                expected.computeIfAbsent(dimensions, (key) -> new CacheStatsCounter());
                for (int j = 0; j < numRepetitionsPerValue; j++) {
                    CacheStatsCounter statsToInc = new CacheStatsCounter(
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
                    expected.get(dimensions).entries.inc(statsToInc.getEntries());
                    StatsHolderTests.populateStatsHolderFromStatsValueMap(statsHolder, Map.of(dimensions, statsToInc));
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

    private void assertSumOfChildrenStats(DimensionNode current) {
        if (!current.children.isEmpty()) {
            CacheStatsCounter expectedTotal = new CacheStatsCounter();
            for (DimensionNode child : current.children.values()) {
                expectedTotal.add(child.getStatsSnapshot());
            }
            assertEquals(expectedTotal.snapshot(), current.getStatsSnapshot());
            for (DimensionNode child : current.children.values()) {
                assertSumOfChildrenStats(child);
            }
        }
    }

    static void populateStatsHolderFromStatsValueMap(StatsHolder statsHolder, Map<List<String>, CacheStatsCounter> statsMap) {
        for (Map.Entry<List<String>, CacheStatsCounter> entry : statsMap.entrySet()) {
            CacheStatsCounter stats = entry.getValue();
            List<String> dims = entry.getKey();
            for (int i = 0; i < stats.getHits(); i++) {
                statsHolder.incrementHits(dims);
            }
            for (int i = 0; i < stats.getMisses(); i++) {
                statsHolder.incrementMisses(dims);
            }
            for (int i = 0; i < stats.getEvictions(); i++) {
                statsHolder.incrementEvictions(dims);
            }
            statsHolder.incrementSizeInBytes(dims, stats.getSizeInBytes());
            for (int i = 0; i < stats.getEntries(); i++) {
                statsHolder.incrementEntries(dims);
            }
        }
    }
}
