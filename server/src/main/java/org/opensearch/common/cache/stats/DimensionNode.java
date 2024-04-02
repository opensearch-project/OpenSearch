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
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * A node in a tree structure, which stores stats in StatsHolder or CacheStats implementations.
 *
 * @param <C> the type of the stats counter in the leaf nodes; could be mutable CacheStatsCounter or immutable CounterSnapshot
 */
class DimensionNode<C> {
    private final String dimensionValue;
    final TreeMap<String, DimensionNode<C>> children; // Map from dimensionValue to the DimensionNode for that dimension value
    private C stats; // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
                     // contains the sum of its children's stats.

    DimensionNode(String dimensionValue) {
        this.dimensionValue = dimensionValue;
        this.children = new TreeMap<>();
        this.stats = null;
    }

    /**
     * Returns the node found by following these dimension values down from the current node.
     * If such a node does not exist, creates it.
     */
    DimensionNode<C> getOrCreateNode(List<String> dimensionValues, Supplier<C> newStatsSupplier) {
        DimensionNode<C> current = this;
        for (String dimensionValue : dimensionValues) {
            current.children.putIfAbsent(dimensionValue, new DimensionNode<C>(dimensionValue));
            current = current.children.get(dimensionValue);
            if (current.stats == null) {
                current.stats = newStatsSupplier.get();
            }
        }
        return current;
    }

    /**
     * Returns the node found by following these dimension values down from the current node.
     * Returns null if no such node exists.
     */
    DimensionNode<C> getNode(List<String> dimensionValues) {
        DimensionNode<C> current = this;
        for (String dimensionValue : dimensionValues) {
            current = current.children.get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    List<DimensionNode<C>> getNodeAndAncestors(List<String> dimensionValues) {
        List<DimensionNode<C>> result = new ArrayList<>();
        result.add(this);
        DimensionNode<C> current = this;
        for (String dimensionValue : dimensionValues) {
            current = current.children.get(dimensionValue);
            if (current == null) {
                return new ArrayList<>(); // Return an empty list if the complete path doesn't exist
            }
            result.add(current);
        }
        return result;
    }

    public C getStats() {
        return stats;
    }

    public void setStats(C stats) {
        this.stats = stats;
    }

    public String getDimensionValue() {
        return dimensionValue;
    }
}
