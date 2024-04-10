/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A node in a tree structure, which stores stats in StatsHolder.
 */
class DimensionNode {
    private final String dimensionValue;
    // Map from dimensionValue to the DimensionNode for that dimension value.
    final Map<String, DimensionNode> children;
    // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
    // contains the sum of its children's stats.
    private CacheStatsCounter stats;

    // Used for leaf nodes to avoid allocating many unnecessary maps
    private static final Map<String, DimensionNode> EMPTY_CHILDREN_MAP = new HashMap<>();

    DimensionNode(String dimensionValue, boolean createChildrenMap) {
        this.dimensionValue = dimensionValue;
        if (createChildrenMap) {
            this.children = new ConcurrentHashMap<>();
        } else {
            this.children = EMPTY_CHILDREN_MAP;
        }
        this.stats = new CacheStatsCounter();
    }

    public String getDimensionValue() {
        return dimensionValue;
    }

    protected Map<String, DimensionNode> getChildren() {
        // We can safely iterate over ConcurrentHashMap without worrying about thread issues.
        return children;
    }

    // Functions for modifying internal CacheStatsCounter without callers having to be aware of CacheStatsCounter

    void incrementHits() {
        this.stats.incrementHits();
    }

    void incrementMisses() {
        this.stats.incrementMisses();
    }

    void incrementEvictions() {
        this.stats.incrementEvictions();
    }

    void incrementSizeInBytes(long amountBytes) {
        this.stats.incrementSizeInBytes(amountBytes);
    }

    void decrementSizeInBytes(long amountBytes) {
        this.stats.decrementSizeInBytes(amountBytes);
    }

    void incrementEntries() {
        this.stats.incrementEntries();
    }

    void decrementEntries() {
        this.stats.decrementEntries();
    }

    long getEntries() {
        return this.stats.getEntries();
    }

    CacheStatsCounterSnapshot getStatsSnapshot() {
        return this.stats.snapshot();
    }

    void decrementBySnapshot(CacheStatsCounterSnapshot snapshot) {
        this.stats.subtract(snapshot);
    }

    void resetSizeAndEntries() {
        this.stats.resetSizeAndEntries();
    }

    DimensionNode getChild(String dimensionValue) {
        return children.get(dimensionValue);
    }

    DimensionNode createChild(String dimensionValue, boolean createMapInChild) {
        return children.computeIfAbsent(dimensionValue, (key) -> new DimensionNode(dimensionValue, createMapInChild));
    }
}
