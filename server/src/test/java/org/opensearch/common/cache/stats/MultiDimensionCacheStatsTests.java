/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class MultiDimensionCacheStatsTests extends OpenSearchTestCase {

    public void testGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = StatsHolderTests.getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = StatsHolderTests.populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        // test the value in the map is as expected for each distinct combination of values
        for (List<String> dimensionValues : expected.keySet()) {
            CacheStatsCounter expectedCounter = expected.get(dimensionValues);

            CacheStatsCounterSnapshot actualStatsHolder = StatsHolderTests.getNode(dimensionValues, statsHolder.getStatsRoot())
                .getStatsSnapshot();
            CacheStatsCounterSnapshot actualCacheStats = getNode(dimensionValues, stats.getStatsRoot()).getStats();

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

        assertSumOfChildrenStats(stats.getStatsRoot());
    }

    public void testEmptyDimsList() throws Exception {
        // If the dimension list is empty, the tree should have only the root node containing the total stats.
        StatsHolder statsHolder = new StatsHolder(List.of());
        Map<String, List<String>> usedDimensionValues = StatsHolderTests.getUsedDimensionValues(statsHolder, 100);
        StatsHolderTests.populateStats(statsHolder, usedDimensionValues, 10, 100);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        MultiDimensionCacheStats.MDCSDimensionNode statsRoot = stats.getStatsRoot();
        assertEquals(0, statsRoot.children.size());
        assertEquals(stats.getTotalStats(), statsRoot.getStats());
    }

    private MultiDimensionCacheStats.MDCSDimensionNode getNode(
        List<String> dimensionValues,
        MultiDimensionCacheStats.MDCSDimensionNode root
    ) {
        MultiDimensionCacheStats.MDCSDimensionNode current = root;
        for (String dimensionValue : dimensionValues) {
            current = current.getChildren().get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private void assertSumOfChildrenStats(MultiDimensionCacheStats.MDCSDimensionNode current) {
        if (!current.children.isEmpty()) {
            CacheStatsCounter expectedTotal = new CacheStatsCounter();
            for (MultiDimensionCacheStats.MDCSDimensionNode child : current.children.values()) {
                expectedTotal.add(child.getStats());
            }
            assertEquals(expectedTotal.snapshot(), current.getStats());
            for (MultiDimensionCacheStats.MDCSDimensionNode child : current.children.values()) {
                assertSumOfChildrenStats(child);
            }
        }
    }
}
