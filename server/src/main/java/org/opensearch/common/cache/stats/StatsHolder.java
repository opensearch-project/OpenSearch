/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.metrics.CounterMetric;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

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
    private final DimensionNode<CacheStatsCounter> statsRoot;

    static final String ROOT_DIMENSION_VALUE = "#ROOT"; // test only for now

    public StatsHolder(List<String> dimensionNames) {
        this.dimensionNames = dimensionNames;
        this.statsRoot = new DimensionNode<CacheStatsCounter>(ROOT_DIMENSION_VALUE); // The root node has no dimension value associated with
                                                                                     // it, only children
        statsRoot.setStats(new CacheStatsCounter());
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    // For all these increment functions, the dimensions list comes from the key, and contains all dimensions present in dimensionNames.
    // The order has to match the order given in dimensionNames.
    public void incrementHits(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.hits.inc(amount), 1);
    }

    public void incrementMisses(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.misses.inc(amount), 1);
    }

    public void incrementEvictions(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.evictions.inc(amount), 1);
    }

    public void incrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.sizeInBytes.inc(amount), amountBytes);
    }

    public void decrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalDecrement(dimensionValues, (counter, amount) -> counter.sizeInBytes.dec(amount), amountBytes);
    }

    public void incrementEntries(List<String> dimensionValues) {
        internalIncrement(dimensionValues, (counter, amount) -> counter.entries.inc(amount), 1);
    }

    public void decrementEntries(List<String> dimensionValues) {
        internalDecrement(dimensionValues, (counter, amount) -> counter.entries.dec(amount), 1);
    }

    // A helper function which traverses the whole stats tree and runs some function taking in the node and path at each node.
    static <C> void traverseStatsTreeHelper(
        DimensionNode<C> currentNode,
        List<String> pathToCurrentNode,
        BiConsumer<DimensionNode<C>, List<String>> function
    ) {
        function.accept(currentNode, pathToCurrentNode);
        if (!currentNode.children.isEmpty()) {
            // not a leaf node
            for (DimensionNode<C> child : currentNode.children.values()) {
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
            if (counter != null) {
                counter.sizeInBytes.dec(counter.getSizeInBytes());
                counter.entries.dec(counter.getEntries());
            }
        });
    }

    public long count() {
        // Include this here so caches don't have to create an entire CacheStats object to run count().
        final CounterMetric count = new CounterMetric();
        traverseStatsTreeHelper(statsRoot, new ArrayList<>(), (node, path) -> {
            if (node.children.isEmpty()) {
                count.inc(node.getStats().getEntries()); // Only increment on leaf nodes to avoid double-counting, as non-leaf nodes contain
                                                         // stats too
            }
        });
        return count.count();
    }

    /**
     * Use the incrementer function to increment a value in the stats for a set of dimensions. If there is no stats
     * for this set of dimensions, create one.
     */
    private void internalIncrement(List<String> dimensionValues, BiConsumer<CacheStatsCounter, Long> incrementer, long amount) {
        assert dimensionValues.size() == dimensionNames.size();
        internalGetOrCreateStats(dimensionValues); // Pass through to ensure all nodes exist before we increment them
        List<DimensionNode<CacheStatsCounter>> ancestors = statsRoot.getNodeAndAncestors(dimensionValues);
        for (DimensionNode<CacheStatsCounter> ancestorNode : ancestors) {
            incrementer.accept(ancestorNode.getStats(), amount);
        }
    }

    /** Similar to internalIncrement, but only applies to existing keys, and does not create a new key if one is absent.
     * This protects us from erroneously decrementing values for keys which have been entirely deleted,
     * for example in an async removal listener.
     */
    private void internalDecrement(List<String> dimensionValues, BiConsumer<CacheStatsCounter, Long> decrementer, long amount) {
        assert dimensionValues.size() == dimensionNames.size();
        List<DimensionNode<CacheStatsCounter>> ancestors = statsRoot.getNodeAndAncestors(dimensionValues);
        for (DimensionNode<CacheStatsCounter> ancestorNode : ancestors) {
            decrementer.accept(ancestorNode.getStats(), amount);
        }
    }

    private CacheStatsCounter internalGetOrCreateStats(List<String> dimensionValues) {
        return statsRoot.getOrCreateNode(dimensionValues, CacheStatsCounter::new).getStats();
    }

    /**
     * Produce an immutable CacheStats representation of these stats.
     */
    public CacheStats getCacheStats() {
        DimensionNode<CounterSnapshot> snapshot = new DimensionNode<>(ROOT_DIMENSION_VALUE);
        traverseStatsTreeHelper(statsRoot, new ArrayList<>(), (node, path) -> {
            if (path.size() > 0) {
                CounterSnapshot nodeSnapshot = node.getStats().snapshot();
                String dimensionValue = path.get(path.size() - 1);
                DimensionNode<CounterSnapshot> newNode = new DimensionNode<>(dimensionValue);
                newNode.setStats(nodeSnapshot);
                DimensionNode<CounterSnapshot> parentNode = snapshot.getNode(path.subList(0, path.size() - 1)); // Get the parent of this
                                                                                                                // node in the new tree
                parentNode.children.put(dimensionValue, newNode);
            }
        });
        snapshot.setStats(statsRoot.getStats().snapshot());
        return new MultiDimensionCacheStats(snapshot, dimensionNames);
    }

    /**
     * Remove the stats for the nodes containing these dimension values in their path.
     * The list of dimension values must have a value for every dimension in the stats holder.
     */
    public void removeDimensions(List<String> dimensionValues) {
        assert dimensionValues.size() == dimensionNames.size();
        List<DimensionNode<CacheStatsCounter>> ancestors = statsRoot.getNodeAndAncestors(dimensionValues);
        // Get the parent of the leaf node to remove
        DimensionNode<CacheStatsCounter> parentNode = ancestors.get(ancestors.size() - 2);
        DimensionNode<CacheStatsCounter> removedNode = ancestors.get(ancestors.size() - 1);
        CacheStatsCounter statsToDecrement = removedNode.getStats();
        if (parentNode != null) {
            parentNode.children.remove(removedNode.getDimensionValue());
        }

        // Now for all nodes that were ancestors of the removed node, decrement their stats, and check if they now have no children. If so,
        // remove them.
        for (int i = dimensionValues.size() - 1; i >= 1; i--) {
            DimensionNode<CacheStatsCounter> currentNode = ancestors.get(i);
            parentNode = ancestors.get(i - 1);
            currentNode.getStats().subtract(statsToDecrement);
            if (currentNode.children.isEmpty()) {
                parentNode.children.remove(currentNode.getDimensionValue());
            }
        }
        // Finally, decrement stats for the root node.
        statsRoot.getStats().subtract(statsToDecrement);
    }

    // pkg-private for testing
    DimensionNode<CacheStatsCounter> getStatsRoot() {
        return statsRoot;
    }
}
