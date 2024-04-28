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

public class ImmutableCacheStatsHolderTests extends OpenSearchTestCase {

    public void testGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        DefaultCacheStatsHolder cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(cacheStatsHolder, 10);
        Map<List<String>, CacheStats> expected = DefaultCacheStatsHolderTests.populateStats(
            cacheStatsHolder,
            usedDimensionValues,
            1000,
            10
        );
        ImmutableCacheStatsHolder stats = cacheStatsHolder.getImmutableCacheStatsHolder();

        // test the value in the map is as expected for each distinct combination of values
        for (List<String> dimensionValues : expected.keySet()) {
            CacheStats expectedCounter = expected.get(dimensionValues);

            ImmutableCacheStats actualCacheStatsHolder = DefaultCacheStatsHolderTests.getNode(
                dimensionValues,
                cacheStatsHolder.getStatsRoot()
            ).getImmutableStats();
            ImmutableCacheStats actualImmutableCacheStatsHolder = getNode(dimensionValues, stats.getStatsRoot()).getStats();

            assertEquals(expectedCounter.immutableSnapshot(), actualCacheStatsHolder);
            assertEquals(expectedCounter.immutableSnapshot(), actualImmutableCacheStatsHolder);
        }

        // test gets for total (this also checks sum-of-children logic)
        CacheStats expectedTotal = new CacheStats();
        for (List<String> dims : expected.keySet()) {
            expectedTotal.add(expected.get(dims));
        }
        assertEquals(expectedTotal.immutableSnapshot(), stats.getTotalStats());

        assertEquals(expectedTotal.getHits(), stats.getTotalHits());
        assertEquals(expectedTotal.getMisses(), stats.getTotalMisses());
        assertEquals(expectedTotal.getEvictions(), stats.getTotalEvictions());
        assertEquals(expectedTotal.getSizeInBytes(), stats.getTotalSizeInBytes());
        assertEquals(expectedTotal.getEntries(), stats.getTotalEntries());

        assertSumOfChildrenStats(stats.getStatsRoot());
    }

    public void testEmptyDimsList() throws Exception {
        // If the dimension list is empty, the tree should have only the root node containing the total stats.
        DefaultCacheStatsHolder cacheStatsHolder = new DefaultCacheStatsHolder(List.of());
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(cacheStatsHolder, 100);
        DefaultCacheStatsHolderTests.populateStats(cacheStatsHolder, usedDimensionValues, 10, 100);
        ImmutableCacheStatsHolder stats = cacheStatsHolder.getImmutableCacheStatsHolder();

        ImmutableCacheStatsHolder.Node statsRoot = stats.getStatsRoot();
        assertEquals(0, statsRoot.children.size());
        assertEquals(stats.getTotalStats(), statsRoot.getStats());
    }

    private ImmutableCacheStatsHolder.Node getNode(List<String> dimensionValues, ImmutableCacheStatsHolder.Node root) {
        ImmutableCacheStatsHolder.Node current = root;
        for (String dimensionValue : dimensionValues) {
            current = current.getChildren().get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private void assertSumOfChildrenStats(ImmutableCacheStatsHolder.Node current) {
        if (!current.children.isEmpty()) {
            CacheStats expectedTotal = new CacheStats();
            for (ImmutableCacheStatsHolder.Node child : current.children.values()) {
                expectedTotal.add(child.getStats());
            }
            assertEquals(expectedTotal.immutableSnapshot(), current.getStats());
            for (ImmutableCacheStatsHolder.Node child : current.children.values()) {
                assertSumOfChildrenStats(child);
            }
        }
    }
}
