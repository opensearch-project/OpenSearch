/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A node in a tree structure, which stores stats in StatsHolder.
 */
class DimensionNode {
    private final String dimensionValue;
    // Map from dimensionValue to the DimensionNode for that dimension value
    ConcurrentHashMap<String, DimensionNode> children;
    // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
    // contains the sum of its children's stats.
    private CacheStatsCounter stats;

    DimensionNode(String dimensionValue) {
        this.dimensionValue = dimensionValue;
        this.children = null; // Lazy load this as needed
        this.stats = new CacheStatsCounter();
    }

    public String getDimensionValue() {
        return dimensionValue;
    }

    protected void createChildrenMap() {
        children = new ConcurrentHashMap<>();
    }

    protected Map<String, DimensionNode> getChildren() {
        // We can safely iterate over ConcurrentHashMap without worrying about thread issues.
        return children;
    }

    public CacheStatsCounter getStats() {
        return stats;
    }

    DimensionNode getOrCreateChild(String dimensionValue, boolean createIfAbsent) {
        if (children == null) {
            createChildrenMap();
        }
        // If we are creating new nodes, put one in the map. Otherwise, the mapping function returns null to leave the map unchanged
        return children.computeIfAbsent(dimensionValue, (key) -> createIfAbsent ? new DimensionNode(dimensionValue) : null);
    }

    public boolean hasChildren() {
        return getChildren() != null && !getChildren().isEmpty();
    }
}
