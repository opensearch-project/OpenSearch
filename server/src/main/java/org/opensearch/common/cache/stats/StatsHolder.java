/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.opensearch.common.cache.stats.MultiDimensionCacheStats.MDCSDimensionNode;

/**
 * A class caches use to internally keep track of their stats across multiple dimensions.
 * Not intended to be exposed outside the cache; for this, use statsHolder.getCacheStats() to create an immutable
 * copy of the current state of the stats.
 * Currently, in the IRC, the stats tracked in a StatsHolder will not appear for empty shards that have had no cache
 * operations done on them yet. This might be changed in the future, by exposing a method to add empty nodes to the
 * tree in StatsHolder in the ICache interface.
 *
 * @opensearch.experimental
 */
public class StatsHolder {

    // The list of permitted dimensions. Should be ordered from "outermost" to "innermost", as you would like to
    // aggregate them in an API response.
    private final List<String> dimensionNames;
    // A tree structure based on dimension values, which stores stats values in its leaf nodes.
    // Non-leaf nodes have stats matching the sum of their children.
    // We use a tree structure, rather than a map with concatenated keys, to save on memory usage. If there are many leaf
    // nodes that share a parent, that parent's dimension value will only be stored once, not many times.
    private final DimensionNode statsRoot;
    // To avoid sync problems, obtain a lock before creating or removing nodes in the stats tree.
    // No lock is needed to edit stats on existing nodes.
    private final Lock lock = new ReentrantLock();

    public StatsHolder(List<String> dimensionNames) {
        this.dimensionNames = Collections.unmodifiableList(dimensionNames);
        this.statsRoot = new DimensionNode("", true); // The root node has the empty string as its dimension value
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    // For all these increment functions, the dimensions list comes from the key, and contains all dimensions present in dimensionNames.
    // The order has to match the order given in dimensionNames.
    public void incrementHits(List<String> dimensionValues) {
        internalIncrement(dimensionValues, DimensionNode::incrementHits, true);
    }

    public void incrementMisses(List<String> dimensionValues) {
        internalIncrement(dimensionValues, DimensionNode::incrementMisses, true);
    }

    public void incrementEvictions(List<String> dimensionValues) {
        internalIncrement(dimensionValues, DimensionNode::incrementEvictions, true);
    }

    public void incrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalIncrement(dimensionValues, (node) -> node.incrementSizeInBytes(amountBytes), true);
    }

    // For decrements, we should not create nodes if they are absent. This protects us from erroneously decrementing values for keys
    // which have been entirely deleted, for example in an async removal listener.
    public void decrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalIncrement(dimensionValues, (node) -> node.decrementSizeInBytes(amountBytes), false);
    }

    public void incrementEntries(List<String> dimensionValues) {
        internalIncrement(dimensionValues, DimensionNode::incrementEntries, true);
    }

    public void decrementEntries(List<String> dimensionValues) {
        internalIncrement(dimensionValues, DimensionNode::decrementEntries, false);
    }

    /**
     * Reset number of entries and memory size when all keys leave the cache, but don't reset hit/miss/eviction numbers.
     * This is in line with the behavior of the existing API when caches are cleared.
     */
    public void reset() {
        resetHelper(statsRoot);
    }

    private void resetHelper(DimensionNode current) {
        current.resetSizeAndEntries();
        for (DimensionNode child : current.children.values()) {
            resetHelper(child);
        }
    }

    public long count() {
        // Include this here so caches don't have to create an entire CacheStats object to run count().
        return statsRoot.getEntries();
    }

    private void internalIncrement(List<String> dimensionValues, Consumer<DimensionNode> adder, boolean createNodesIfAbsent) {
        assert dimensionValues.size() == dimensionNames.size();
        // First try to increment without creating nodes
        boolean didIncrement = internalIncrementHelper(dimensionValues, statsRoot, 0, adder, false);
        // If we failed to increment, because nodes had to be created, obtain the lock and run again while creating nodes if needed
        if (!didIncrement) {
            try {
                lock.lock();
                internalIncrementHelper(dimensionValues, statsRoot, 0, adder, createNodesIfAbsent);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Use the incrementer function to increment/decrement a value in the stats for a set of dimensions.
     * If createNodesIfAbsent is true, and there is no stats for this set of dimensions, create one.
     * Returns true if the increment was applied, false if not.
     */
    private boolean internalIncrementHelper(
        List<String> dimensionValues,
        DimensionNode node,
        int depth, // Pass in the depth to avoid having to slice the list for each node.
        Consumer<DimensionNode> adder,
        boolean createNodesIfAbsent
    ) {
        if (depth == dimensionValues.size()) {
            // This is the leaf node we are trying to reach
            adder.accept(node);
            return true;
        }

        DimensionNode child = node.getChild(dimensionValues.get(depth));
        if (child == null) {
            if (createNodesIfAbsent) {
                boolean createMapInChild = depth < dimensionValues.size() - 1;
                child = node.createChild(dimensionValues.get(depth), createMapInChild);
            } else {
                return false;
            }
        }
        if (internalIncrementHelper(dimensionValues, child, depth + 1, adder, createNodesIfAbsent)) {
            // Function returns true if the next node down was incremented
            adder.accept(node);
            return true;
        }
        return false;
    }

    /**
     * Produce an immutable CacheStats representation of these stats.
     */
    public CacheStats getCacheStats() {
        MDCSDimensionNode snapshot = new MDCSDimensionNode("", true, statsRoot.getStatsSnapshot());
        // Traverse the tree and build a corresponding tree of MDCSDimensionNode, to pass to MultiDimensionCacheStats.
        if (statsRoot.getChildren() != null) {
            for (DimensionNode child : statsRoot.getChildren().values()) {
                getCacheStatsHelper(child, snapshot);
            }
        }
        return new MultiDimensionCacheStats(snapshot, dimensionNames);
    }

    private void getCacheStatsHelper(DimensionNode currentNodeInOriginalTree, MDCSDimensionNode parentInNewTree) {
        MDCSDimensionNode newNode = createMatchingMDCSDimensionNode(currentNodeInOriginalTree);
        parentInNewTree.getChildren().put(newNode.getDimensionValue(), newNode);
        for (DimensionNode child : currentNodeInOriginalTree.children.values()) {
            getCacheStatsHelper(child, newNode);
        }
    }

    private MDCSDimensionNode createMatchingMDCSDimensionNode(DimensionNode node) {
        CacheStatsCounterSnapshot nodeSnapshot = node.getStatsSnapshot();
        boolean isLeafNode = node.getChildren().isEmpty();
        MDCSDimensionNode newNode = new MDCSDimensionNode(node.getDimensionValue(), !isLeafNode, nodeSnapshot);
        return newNode;
    }

    public void removeDimensions(List<String> dimensionValues) {
        assert dimensionValues.size() == dimensionNames.size() : "Must specify a value for every dimension when removing from StatsHolder";
        // As we are removing nodes from the tree, obtain the lock
        lock.lock();
        try {
            removeDimensionsHelper(dimensionValues, statsRoot, 0);
        } finally {
            lock.unlock();
        }
    }

    // Returns a CacheStatsCounterSnapshot object for the stats to decrement if the removal happened, null otherwise.
    private CacheStatsCounterSnapshot removeDimensionsHelper(List<String> dimensionValues, DimensionNode node, int depth) {
        if (depth == dimensionValues.size()) {
            // Pass up a snapshot of the original stats to avoid issues when the original is decremented by other fn invocations
            return node.getStatsSnapshot();
        }
        DimensionNode child = node.getChild(dimensionValues.get(depth));
        if (child == null) {
            return null;
        }
        CacheStatsCounterSnapshot statsToDecrement = removeDimensionsHelper(dimensionValues, child, depth + 1);
        if (statsToDecrement != null) {
            // The removal took place, decrement values and remove this node from its parent if it's now empty
            node.decrementBySnapshot(statsToDecrement);
            if (child.getChildren().isEmpty()) {
                node.children.remove(child.getDimensionValue());
            }
        }
        return statsToDecrement;
    }

    // pkg-private for testing
    DimensionNode getStatsRoot() {
        return statsRoot;
    }
}
