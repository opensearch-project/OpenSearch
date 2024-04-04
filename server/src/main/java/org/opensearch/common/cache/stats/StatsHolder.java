/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.opensearch.common.cache.stats.MultiDimensionCacheStats.MDCSDimensionNode;

/**
 * A class caches use to internally keep track of their stats across multiple dimensions.
 * Not intended to be exposed outside the cache; for this, use statsHolder.getCacheStats() to create an immutable
 * copy of the current state of the stats.
 *
 * @opensearch.experimental
 */
public class StatsHolder {

    // The list of permitted dimensions. Should be ordered from "outermost" to "innermost", as you would like to
    // aggregate them in an API response.
    private final List<String> dimensionNames;

    // A tree structure based on dimension values, which stores stats values in its leaf nodes.
    // Non-leaf nodes have stats matching the sum of their children.
    private final StatsHolderDimensionNode statsRoot;

    public StatsHolder(List<String> dimensionNames) {
        this.dimensionNames = dimensionNames;
        this.statsRoot = new StatsHolderDimensionNode(null); // The root node has no dimension value associated with it, only children
        statsRoot.createChildrenMap();
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    // For all these increment functions, the dimensions list comes from the key, and contains all dimensions present in dimensionNames.
    // The order has to match the order given in dimensionNames.
    public void incrementHits(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter) -> counter.hits.inc(), true);
    }

    public void incrementMisses(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter) -> counter.misses.inc(), true);
    }

    public void incrementEvictions(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter) -> counter.evictions.inc(), true);
    }

    public void incrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalIncrement(dimensionValues, (counter) -> counter.sizeInBytes.inc(amountBytes), true);
    }

    // For decrements, we should not create nodes if they are absent. This protects us from erroneously decrementing values for keys
    // which have been entirely deleted, for example in an async removal listener.
    public void decrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalIncrement(dimensionValues, (counter) -> counter.sizeInBytes.dec(amountBytes), false);
    }

    public void incrementEntries(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter) -> counter.entries.inc(), true);
    }

    public void decrementEntries(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter) -> counter.entries.dec(), false);
    }

    // A helper function which traverses the whole stats tree and runs some function taking in the node and path at each node.
    static void traverseStatsTreeHelper(
        StatsHolderDimensionNode currentNode,
        List<String> pathToCurrentNode,
        BiConsumer<StatsHolderDimensionNode, List<String>> function
    ) {
        function.accept(currentNode, pathToCurrentNode);
        if (currentNode.hasChildren()) {
            // not a leaf node
            for (StatsHolderDimensionNode child : currentNode.children.values()) {
                List<String> pathToChild = new ArrayList<>(pathToCurrentNode);
                pathToChild.add(child.getDimensionValue());
                traverseStatsTreeHelper(child, pathToChild, function);
            }
        }
    }

    /**
     * Reset number of entries and memory size when all keys leave the cache, but don't reset hit/miss/eviction numbers.
     * This is in line with the behavior of the existing API when caches are cleared.
     */
    public void reset() {
        traverseStatsTreeHelper(statsRoot, new ArrayList<>(), (node, path) -> {
            CacheStatsCounter counter = node.getStats();
            counter.sizeInBytes.dec(counter.getSizeInBytes());
            counter.entries.dec(counter.getEntries());
        });
    }

    public long count() {
        // Include this here so caches don't have to create an entire CacheStats object to run count().
        return statsRoot.getStats().getEntries();
    }

    private void internalIncrement(List<String> dimensionValues, Consumer<CacheStatsCounter> adder, boolean createNodesIfAbsent) {
        assert dimensionValues.size() == dimensionNames.size();
        boolean didIncrement = internalIncrementHelper(dimensionValues, statsRoot, 0, adder, createNodesIfAbsent);
        if (didIncrement) {
            adder.accept(statsRoot.getStats());
        }
    }

    /**
     * Use the incrementer function to increment/decrement a value in the stats for a set of dimensions.
     * If createNodesIfAbsent is true, and there is no stats for this set of dimensions, create one.
     * Returns true if the increment was applied, false if not.
     */
    private boolean internalIncrementHelper(
        List<String> dimensionValues,
        StatsHolderDimensionNode node,
        int dimensionValuesIndex,
        Consumer<CacheStatsCounter> adder,
        boolean createNodesIfAbsent
    ) {
        if (dimensionValuesIndex == dimensionValues.size()) {
            return true;
        }
        StatsHolderDimensionNode child = node.getOrCreateChild(dimensionValues.get(dimensionValuesIndex), createNodesIfAbsent);
        if (child == null) {
            return false;
        }
        if (internalIncrementHelper(dimensionValues, child, dimensionValuesIndex + 1, adder, createNodesIfAbsent)) {
            adder.accept(child.getStats());
            return true;
        }
        return false;
    }

    /**
     * Produce an immutable CacheStats representation of these stats.
     */
    public CacheStats getCacheStats() {
        MDCSDimensionNode snapshot = new MDCSDimensionNode(null, statsRoot.getStats().snapshot());
        snapshot.createChildrenMap();
        // Traverse the tree and build a corresponding tree of MDCSDimensionNode, to pass to MultiDimensionCacheStats.
        traverseStatsTreeHelper(statsRoot, new ArrayList<>(), (node, path) -> {
            if (path.size() > 0) {
                MDCSDimensionNode newNode = createMatchingMDCSDimensionNode(node);
                // Get the parent of this node in the new tree
                MDCSDimensionNode parentNode = (MDCSDimensionNode) getNode(path.subList(0, path.size() - 1), snapshot);
                parentNode.getChildren().put(node.getDimensionValue(), newNode);
            }
        });
        return new MultiDimensionCacheStats(snapshot, dimensionNames);
    }

    private MDCSDimensionNode createMatchingMDCSDimensionNode(StatsHolderDimensionNode node) {
        CacheStatsCounterSnapshot nodeSnapshot = node.getStats().snapshot();
        MDCSDimensionNode newNode = new MDCSDimensionNode(node.getDimensionValue(), nodeSnapshot);
        if (node.getChildren() != null) {
            newNode.createChildrenMap();
        }
        return newNode;
    }

    public void removeDimensions(List<String> dimensionValues) {
        assert dimensionValues.size() == dimensionNames.size() : "Must specify a value for every dimension when removing from StatsHolder";
        CacheStatsCounterSnapshot statsToDecrement = removeDimensionsHelper(dimensionValues, statsRoot, 0);
        if (statsToDecrement != null) {
            statsRoot.getStats().subtract(statsToDecrement);
        }
    }

    // Returns a CacheStatsCounter object for the stats to decrement if the removal happened, null otherwise.
    private CacheStatsCounterSnapshot removeDimensionsHelper(
        List<String> dimensionValues,
        StatsHolderDimensionNode node,
        int dimensionValuesIndex
    ) {
        if (dimensionValuesIndex == dimensionValues.size()) {
            // Pass up a snapshot of the original stats to avoid issues when the original is decremented by other fn invocations
            return node.getStats().snapshot();
        }
        StatsHolderDimensionNode child = node.getOrCreateChild(dimensionValues.get(dimensionValuesIndex), false);
        if (child == null) {
            return null;
        }
        CacheStatsCounterSnapshot statsToDecrement = removeDimensionsHelper(dimensionValues, child, dimensionValuesIndex + 1);
        if (statsToDecrement != null) {
            // The removal took place, decrement values and remove this node from its parent if it's now empty
            child.getStats().subtract(statsToDecrement);
            if (child.getStats().isZero()) {
                node.children.remove(child.getDimensionValue());
            }
        }
        return statsToDecrement;
    }

    static class StatsHolderDimensionNode extends DimensionNode {
        // Map from dimensionValue to the DimensionNode for that dimension value
        ConcurrentHashMap<String, StatsHolderDimensionNode> children;
        // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
        // contains the sum of its children's stats.
        private CacheStatsCounter stats;

        StatsHolderDimensionNode(String dimensionValue) {
            super(dimensionValue);
            this.children = null; // Lazy load this as needed
            this.stats = new CacheStatsCounter();
        }

        @Override
        protected void createChildrenMap() {
            children = new ConcurrentHashMap<>();
        }

        @Override
        protected Map<String, StatsHolderDimensionNode> getChildren() {
            // We can safely iterate over ConcurrentHashMap without worrying about thread issues.
            return children;
        }

        public CacheStatsCounter getStats() {
            return stats;
        }

        StatsHolderDimensionNode getOrCreateChild(String dimensionValue, boolean createIfAbsent) {
            if (children == null) {
                createChildrenMap();
            }
            // If we are creating new nodes, put one in the map. Otherwise, the mapping function returns null to leave the map unchanged
            return children.computeIfAbsent(dimensionValue, (key) -> createIfAbsent ? new StatsHolderDimensionNode(dimensionValue) : null);
        }
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

    // pkg-private for testing
    StatsHolderDimensionNode getStatsRoot() {
        return statsRoot;
    }
}
