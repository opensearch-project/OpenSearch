/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A CacheStats object supporting aggregation over multiple different dimensions.
 * Stores a fixed snapshot of a cache's stats; does not allow changes.
 *
 * @opensearch.experimental
 */
public class MultiDimensionCacheStats implements CacheStats {
    // A snapshot of a StatsHolder containing stats maintained by the cache.
    // Pkg-private for testing.
    final MDCSDimensionNode statsRoot;
    final List<String> dimensionNames;

    public MultiDimensionCacheStats(MDCSDimensionNode statsRoot, List<String> dimensionNames) {
        this.statsRoot = statsRoot;
        this.dimensionNames = dimensionNames;
    }

    @Override
    public CacheStatsCounterSnapshot getTotalStats() {
        return statsRoot.getStats();
    }

    @Override
    public long getTotalHits() {
        return getTotalStats().getHits();
    }

    @Override
    public long getTotalMisses() {
        return getTotalStats().getMisses();
    }

    @Override
    public long getTotalEvictions() {
        return getTotalStats().getEvictions();
    }

    @Override
    public long getTotalSizeInBytes() {
        return getTotalStats().getSizeInBytes();
    }

    @Override
    public long getTotalEntries() {
        return getTotalStats().getEntries();
    }

    public CacheStatsCounterSnapshot getStatsForDimensionValues(List<String> dimensionValues) {
        MDCSDimensionNode current = statsRoot;
        for (String dimensionValue : dimensionValues) {
            current = current.children.get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current.stats;
    }

    // A similar class to DimensionNode, which uses an ordered TreeMap and holds immutable CacheStatsCounterSnapshot as its stats.
    static class MDCSDimensionNode {
        private final String dimensionValue;
        final Map<String, MDCSDimensionNode> children; // Map from dimensionValue to the DimensionNode for that dimension value

        // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
        // contains the sum of its children's stats.
        private CacheStatsCounterSnapshot stats;
        private static final Map<String, MDCSDimensionNode> EMPTY_CHILDREN_MAP = new HashMap<>();

        MDCSDimensionNode(String dimensionValue, boolean createChildrenMap, CacheStatsCounterSnapshot stats) {
            this.dimensionValue = dimensionValue;
            if (createChildrenMap) {
                this.children = new TreeMap<>(); // This map should be ordered to enforce a consistent order in API response
            } else {
                this.children = EMPTY_CHILDREN_MAP;
            }
            this.stats = stats;
        }

        MDCSDimensionNode(String dimensionValue, boolean createChildrenMap) {
            this(dimensionValue, createChildrenMap, null);
        }

        Map<String, MDCSDimensionNode> getChildren() {
            return children;
        }

        public CacheStatsCounterSnapshot getStats() {
            return stats;
        }

        public void setStats(CacheStatsCounterSnapshot stats) {
            this.stats = stats;
        }

        public String getDimensionValue() {
            return dimensionValue;
        }
    }

    // pkg-private for testing
    MDCSDimensionNode getStatsRoot() {
        return statsRoot;
    }

    // TODO (in API PR): Produce XContent based on aggregateByLevels()
}
