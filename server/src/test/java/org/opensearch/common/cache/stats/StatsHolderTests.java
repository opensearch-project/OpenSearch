/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests.getUsedDimensionValues;
import static org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests.populateStats;

public class StatsHolderTests extends OpenSearchTestCase {
    // Since StatsHolder does not expose getter methods for aggregating stats,
    // we test the incrementing functionality in combination with MultiDimensionCacheStats,
    // in MultiDimensionCacheStatsTests.java.

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
}
