/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * An object storing an immutable snapshot of an entire cache's stats. Accessible outside the cache itself.
 *
 * @opensearch.experimental
 */

@ExperimentalApi
public class ImmutableCacheStatsHolder { // TODO: extends Writeable, ToXContent
    // An immutable snapshot of a stats within a CacheStatsHolder, containing all the stats maintained by the cache.
    // Pkg-private for testing.
    final Node statsRoot;
    final List<String> dimensionNames;

    public ImmutableCacheStatsHolder(Node statsRoot, List<String> dimensionNames) {
        this.statsRoot = statsRoot;
        this.dimensionNames = dimensionNames;
    }

    public ImmutableCacheStats getTotalStats() {
        return statsRoot.getStats();
    }

    public long getTotalHits() {
        return getTotalStats().getHits();
    }

    public long getTotalMisses() {
        return getTotalStats().getMisses();
    }

    public long getTotalEvictions() {
        return getTotalStats().getEvictions();
    }

    public long getTotalSizeInBytes() {
        return getTotalStats().getSizeInBytes();
    }

    public long getTotalEntries() {
        return getTotalStats().getEntries();
    }

    public ImmutableCacheStats getStatsForDimensionValues(List<String> dimensionValues) {
        Node current = statsRoot;
        for (String dimensionValue : dimensionValues) {
            current = current.children.get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current.stats;
    }

    // A similar class to CacheStatsHolder.Node, which uses an ordered TreeMap and holds immutable CacheStatsSnapshot as its stats.
    static class Node {
        private final String dimensionValue;
        final Map<String, Node> children; // Map from dimensionValue to the Node for that dimension value

        // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
        // contains the sum of its children's stats.
        private final ImmutableCacheStats stats;
        private static final Map<String, Node> EMPTY_CHILDREN_MAP = new HashMap<>();

        Node(String dimensionValue, TreeMap<String, Node> snapshotChildren, ImmutableCacheStats stats) {
            this.dimensionValue = dimensionValue;
            this.stats = stats;
            if (snapshotChildren == null) {
                this.children = EMPTY_CHILDREN_MAP;
            } else {
                this.children = Collections.unmodifiableMap(snapshotChildren);
            }
        }

        Map<String, Node> getChildren() {
            return children;
        }

        public ImmutableCacheStats getStats() {
            return stats;
        }

        public String getDimensionValue() {
            return dimensionValue;
        }
    }

    // pkg-private for testing
    Node getStatsRoot() {
        return statsRoot;
    }

    // TODO (in API PR): Produce XContent based on aggregateByLevels()
}
