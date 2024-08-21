/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A class ICache implementations use to internally keep track of their stats across multiple dimensions.
 * Not intended to be exposed outside the cache; for this, caches use getImmutableCacheStatsHolder() to create an immutable
 * copy of the current state of the stats.
 * Currently, in the IRC, the stats tracked in a CacheStatsHolder will not appear for empty shards that have had no cache
 * operations done on them yet. This might be changed in the future, by exposing a method to add empty nodes to the
 * tree in CacheStatsHolder in the ICache interface.
 *
 * @opensearch.experimental
 */
public class DefaultCacheStatsHolder implements CacheStatsHolder {

    // The list of permitted dimensions. Should be ordered from "outermost" to "innermost", as you would like to
    // aggregate them in an API response.
    protected final List<String> dimensionNames;
    // A tree structure based on dimension values, which stores stats values in its leaf nodes.
    // Non-leaf nodes have stats matching the sum of their children.
    // We use a tree structure, rather than a map with concatenated keys, to save on memory usage. If there are many leaf
    // nodes that share a parent, that parent's dimension value will only be stored once, not many times.
    private final Node statsRoot;
    // To avoid sync problems, obtain a lock before creating or removing nodes in the stats tree.
    // No lock is needed to edit stats on existing nodes.
    private final Lock lock = new ReentrantLock();
    // The name of the cache type using these stats
    private final String storeName;

    public DefaultCacheStatsHolder(List<String> dimensionNames, String storeName) {
        this.dimensionNames = Collections.unmodifiableList(dimensionNames);
        this.storeName = storeName;
        this.statsRoot = new Node("", true); // The root node has the empty string as its dimension value
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    // For all these increment functions, the dimensions list comes from the key, and contains all dimensions present in dimensionNames.
    // The order has to match the order given in dimensionNames.
    @Override
    public void incrementHits(List<String> dimensionValues) {
        internalIncrement(dimensionValues, Node::incrementHits, true);
    }

    @Override
    public void incrementMisses(List<String> dimensionValues) {
        internalIncrement(dimensionValues, Node::incrementMisses, true);
    }

    @Override
    public void incrementEvictions(List<String> dimensionValues) {
        internalIncrement(dimensionValues, Node::incrementEvictions, true);
    }

    @Override
    public void incrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalIncrement(dimensionValues, (node) -> node.incrementSizeInBytes(amountBytes), true);
    }

    // For decrements, we should not create nodes if they are absent. This protects us from erroneously decrementing values for keys
    // which have been entirely deleted, for example in an async removal listener.
    @Override
    public void decrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        internalIncrement(dimensionValues, (node) -> node.decrementSizeInBytes(amountBytes), false);
    }

    @Override
    public void incrementItems(List<String> dimensionValues) {
        internalIncrement(dimensionValues, Node::incrementItems, true);
    }

    @Override
    public void decrementItems(List<String> dimensionValues) {
        internalIncrement(dimensionValues, Node::decrementItems, false);
    }

    /**
     * Reset number of entries and memory size when all keys leave the cache, but don't reset hit/miss/eviction numbers.
     * This is in line with the behavior of the existing API when caches are cleared.
     */
    @Override
    public void reset() {
        resetHelper(statsRoot);
    }

    private void resetHelper(Node current) {
        current.resetSizeAndEntries();
        for (Node child : current.children.values()) {
            resetHelper(child);
        }
    }

    @Override
    public long count() {
        // Include this here so caches don't have to create an entire CacheStats object to run count().
        return statsRoot.getEntries();
    }

    protected void internalIncrement(List<String> dimensionValues, Consumer<Node> adder, boolean createNodesIfAbsent) {
        assert dimensionValues.size() == dimensionNames.size();
        // First try to increment without creating nodes
        boolean didIncrement = internalIncrementHelper(dimensionValues, statsRoot, 0, adder, false);
        // If we failed to increment, because nodes had to be created, obtain the lock and run again while creating nodes if needed
        if (!didIncrement && createNodesIfAbsent) {
            try {
                lock.lock();
                internalIncrementHelper(dimensionValues, statsRoot, 0, adder, true);
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
        Node node,
        int depth, // Pass in the depth to avoid having to slice the list for each node.
        Consumer<Node> adder,
        boolean createNodesIfAbsent
    ) {
        if (depth == dimensionValues.size()) {
            // This is the leaf node we are trying to reach
            adder.accept(node);
            return true;
        }

        Node child = node.getChild(dimensionValues.get(depth));
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
     * Produce an immutable version of these stats, aggregated according to levels.
     * If levels is null, do not aggregate and return an immutable version of the original tree.
     */
    @Override
    public ImmutableCacheStatsHolder getImmutableCacheStatsHolder(String[] levels) {
        String[] nonNullLevels = Objects.requireNonNullElseGet(levels, () -> new String[0]);
        return new ImmutableCacheStatsHolder(this.statsRoot, nonNullLevels, dimensionNames, storeName);
    }

    @Override
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
    private ImmutableCacheStats removeDimensionsHelper(List<String> dimensionValues, Node node, int depth) {
        if (depth == dimensionValues.size()) {
            // Pass up a snapshot of the original stats to avoid issues when the original is decremented by other fn invocations
            return node.getImmutableStats();
        }
        Node child = node.getChild(dimensionValues.get(depth));
        if (child == null) {
            return null;
        }
        ImmutableCacheStats statsToDecrement = removeDimensionsHelper(dimensionValues, child, depth + 1);
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
    Node getStatsRoot() {
        return statsRoot;
    }

    /**
     * Nodes that make up the tree in the stats holder.
     */
    protected static class Node {
        private final String dimensionValue;
        // Map from dimensionValue to the DimensionNode for that dimension value.
        final Map<String, Node> children;
        // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
        // contains the sum of its children's stats.
        private CacheStats stats;

        // Used for leaf nodes to avoid allocating many unnecessary maps
        private static final Map<String, Node> EMPTY_CHILDREN_MAP = new HashMap<>();

        Node(String dimensionValue, boolean createChildrenMap) {
            this.dimensionValue = dimensionValue;
            if (createChildrenMap) {
                this.children = new ConcurrentHashMap<>();
            } else {
                this.children = EMPTY_CHILDREN_MAP;
            }
            this.stats = new CacheStats();
        }

        public String getDimensionValue() {
            return dimensionValue;
        }

        protected Map<String, Node> getChildren() {
            // We can safely iterate over ConcurrentHashMap without worrying about thread issues.
            return children;
        }

        // Functions for modifying internal CacheStatsCounter without callers having to be aware of CacheStatsCounter

        public void incrementHits() {
            this.stats.incrementHits();
        }

        public void incrementMisses() {
            this.stats.incrementMisses();
        }

        public void incrementEvictions() {
            this.stats.incrementEvictions();
        }

        public void incrementSizeInBytes(long amountBytes) {
            this.stats.incrementSizeInBytes(amountBytes);
        }

        public void decrementSizeInBytes(long amountBytes) {
            this.stats.decrementSizeInBytes(amountBytes);
        }

        void incrementItems() {
            this.stats.incrementItems();
        }

        void decrementItems() {
            this.stats.decrementItems();
        }

        long getEntries() {
            return this.stats.getItems();
        }

        ImmutableCacheStats getImmutableStats() {
            return this.stats.immutableSnapshot();
        }

        void decrementBySnapshot(ImmutableCacheStats snapshot) {
            this.stats.subtract(snapshot);
        }

        void resetSizeAndEntries() {
            this.stats.resetSizeAndEntries();
        }

        Node getChild(String dimensionValue) {
            return children.get(dimensionValue);
        }

        Node createChild(String dimensionValue, boolean createMapInChild) {
            return children.computeIfAbsent(dimensionValue, (key) -> new Node(dimensionValue, createMapInChild));
        }

        /**
         * Return whether this is a leaf node which is at the lowest level of the tree.
         * Does not return true if this is a node at a higher level whose children are still being constructed.
         * @return if this is a leaf node at the lowest level
         */
        public boolean isAtLowestLevel() {
            // Compare by value to the empty children map, to ensure we don't get false positives for nodes
            // which are in the process of having children added
            return children == EMPTY_CHILDREN_MAP;
        }
    }
}
