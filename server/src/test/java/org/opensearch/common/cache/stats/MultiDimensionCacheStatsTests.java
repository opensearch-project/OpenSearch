/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultiDimensionCacheStatsTests extends OpenSearchTestCase {
    public void testSerialization() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = StatsHolderTests.getUsedDimensionValues(statsHolder, 10);
        StatsHolderTests.populateStats(statsHolder, usedDimensionValues, 100, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        MultiDimensionCacheStats deserialized = new MultiDimensionCacheStats(is);

        assertEquals(stats.dimensionNames, deserialized.dimensionNames);
        List<List<String>> pathsInOriginal = new ArrayList<>();
        getAllPathsInTree(stats.getStatsRoot(), new ArrayList<>(), pathsInOriginal);
        for (List<String> path : pathsInOriginal) {
            MultiDimensionCacheStats.MDCSDimensionNode originalNode = getNode(path, stats.statsRoot);
            MultiDimensionCacheStats.MDCSDimensionNode deserializedNode = getNode(path, deserialized.statsRoot);
            assertNotNull(deserializedNode);
            assertEquals(originalNode.getDimensionValue(), deserializedNode.getDimensionValue());
            assertEquals(originalNode.getStats(), deserializedNode.getStats());
        }
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

    private void getAllPathsInTree(
        MultiDimensionCacheStats.MDCSDimensionNode currentNode,
        List<String> pathToCurrentNode,
        List<List<String>> allPaths
    ) {
        allPaths.add(pathToCurrentNode);
        if (currentNode.getChildren() != null && !currentNode.getChildren().isEmpty()) {
            // not a leaf node
            for (MultiDimensionCacheStats.MDCSDimensionNode child : currentNode.getChildren().values()) {
                List<String> pathToChild = new ArrayList<>(pathToCurrentNode);
                pathToChild.add(child.getDimensionValue());
                getAllPathsInTree(child, pathToChild, allPaths);
            }
        }
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
}
