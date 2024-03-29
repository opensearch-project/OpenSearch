/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
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
    final Map<StatsHolder.Key, CounterSnapshot> snapshot;
    final List<String> dimensionNames;

    public MultiDimensionCacheStats(Map<StatsHolder.Key, CounterSnapshot> snapshot, List<String> dimensionNames) {
        this.snapshot = snapshot;
        this.dimensionNames = dimensionNames;
    }

    public MultiDimensionCacheStats(StreamInput in) throws IOException {
        this.dimensionNames = List.of(in.readStringArray());
        this.snapshot = in.readMap(
            i -> new StatsHolder.Key(List.of(i.readArray(CacheStatsDimension::new, CacheStatsDimension[]::new))),
            CounterSnapshot::new
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        out.writeMap(
            snapshot,
            (o, key) -> o.writeArray((o1, dim) -> ((CacheStatsDimension) dim).writeTo(o1), key.dimensions.toArray()),
            (o, snapshot) -> snapshot.writeTo(o)
        );
    }

    @Override
    public CounterSnapshot getTotalStats() {
        CacheStatsCounter counter = new CacheStatsCounter();
        // To avoid making many Snapshot objects for the incremental sums, add to a mutable CacheStatsCounter and finally convert to
        // Snapshot
        for (CounterSnapshot snapshotValue : snapshot.values()) {
            counter.add(snapshotValue);
        }
        return counter.snapshot();
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

    static class DimensionNode {
        private final String dimensionValue;
        // Storing dimensionValue is useful for producing XContent
        final TreeMap<String, DimensionNode> children; // Map from dimensionValue to the DimensionNode for that dimension value
        private CounterSnapshot snapshot;

        DimensionNode(String dimensionValue) {
            this.dimensionValue = dimensionValue;
            this.children = new TreeMap<>();
            this.snapshot = null;
            // Only leaf nodes have non-null snapshots. Might make it be sum-of-children in future.
        }

        /**
         * Increments the snapshot in this node.
         */
        void addSnapshot(CounterSnapshot newSnapshot) {
            if (snapshot == null) {
                snapshot = newSnapshot;
            } else {
                snapshot = CounterSnapshot.addSnapshots(snapshot, newSnapshot);
            }
        }

        /**
         * Returns the node found by following these dimension values down from the current node.
         * If such a node does not exist, creates it.
         */
        DimensionNode getNode(List<String> dimensionValues) {
            DimensionNode current = this;
            for (String dimensionValue : dimensionValues) {
                current.children.putIfAbsent(dimensionValue, new DimensionNode(dimensionValue));
                current = current.children.get(dimensionValue);
            }
            return current;
        }

        CounterSnapshot getSnapshot() {
            return snapshot;
        }
    }

    /**
     * Returns a tree containing the stats aggregated by the levels passed in. The root node is a dummy node,
     * whose name and value are null.
     */
    DimensionNode aggregateByLevels(List<String> levels) {
        int[] levelPositions = getLevelsInSortedOrder(levels); // Check validity of levels and get their indices in dimensionNames

        DimensionNode root = new DimensionNode(null);
        for (Map.Entry<StatsHolder.Key, CounterSnapshot> entry : snapshot.entrySet()) {
            List<String> levelValues = new ArrayList<>(); // This key's relevant dimension values, which match the levels
            List<CacheStatsDimension> keyDimensions = entry.getKey().dimensions;
            for (int levelPosition : levelPositions) {
                levelValues.add(keyDimensions.get(levelPosition).dimensionValue);
            }
            DimensionNode leafNode = root.getNode(levelValues);
            leafNode.addSnapshot(entry.getValue());
        }
        return root;
    }

    private int[] getLevelsInSortedOrder(List<String> levels) {
        // Levels must all be present in dimensionNames and also be in matching order, or they are invalid
        // Return an array of each level's position within the list dimensionNames
        if (levels.isEmpty()) {
            throw new IllegalArgumentException("Levels cannot have size 0");
        }
        int[] result = new int[levels.size()];
        for (int i = 0; i < levels.size(); i++) {
            String level = levels.get(i);
            int levelIndex = dimensionNames.indexOf(level);
            if (levelIndex != -1) {
                result[i] = levelIndex;
            } else {
                throw new IllegalArgumentException("Unrecognized level: " + level);
            }
            if (i > 0 && result[i] < result[i - 1]) {
                // If the levels passed in are out of order, they are invalid
                throw new IllegalArgumentException("Invalid ordering for levels: " + levels);
            }
        }
        return result;
    }

    // TODO (in API PR): Produce XContent based on aggregateByLevels()
}
