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
import java.util.Comparator;
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
            i -> new StatsHolder.Key(List.of(i.readArray(StreamInput::readString, String[]::new))),
            CounterSnapshot::new
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        out.writeMap(
            snapshot,
            (o, key) -> o.writeArray((o1, dimValue) -> o1.writeString((String) dimValue), key.dimensionValues.toArray()),
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

    /**
     * Return a TreeMap containing stats values aggregated by the levels passed in. Results are ordered so that
     * values are grouped by their dimension values, which matches the order they should be outputted in an API response.
     * Example: if the dimension names are "indices", "shards", and "tier", and levels are "indices" and "shards", it
     * groups the stats by indices and shard values and returns them in order.
     * Pkg-private for testing.
     * @param levels The levels to aggregate by
     * @return The resulting stats
     */
    TreeMap<StatsHolder.Key, CounterSnapshot> aggregateByLevels(List<String> levels) {
        int[] levelPositions = getLevelsInSortedOrder(levels); // Check validity of levels and get their indices in dimensionNames
        TreeMap<StatsHolder.Key, CounterSnapshot> result = new TreeMap<>(new KeyComparator());

        for (Map.Entry<StatsHolder.Key, CounterSnapshot> entry : snapshot.entrySet()) {
            List<String> levelValues = new ArrayList<>(); // This key's relevant dimension values, which match the levels
            List<String> keyDimensionValues = entry.getKey().dimensionValues;
            for (int levelPosition : levelPositions) {
                levelValues.add(keyDimensionValues.get(levelPosition));
            }
            // The new keys, for the aggregated stats, contain only the dimensions specified in levels
            StatsHolder.Key levelsKey = new StatsHolder.Key(levelValues);
            CounterSnapshot originalCounter = entry.getValue();
            // Increment existing key in aggregation with this value, or create a new one if it's not present.
            result.compute(
                levelsKey,
                (k, v) -> (v == null) ? originalCounter : CounterSnapshot.addSnapshots(result.get(levelsKey), originalCounter)
            );
        }
        return result;
    }

    public TreeMap<StatsHolder.Key, CounterSnapshot> getSortedMap() {
        TreeMap<StatsHolder.Key, CounterSnapshot> result = new TreeMap<>(new KeyComparator());
        result.putAll(snapshot);
        return result;
    }

    // First compare outermost dimension, then second outermost, etc.
    // Pkg-private for testing
    static class KeyComparator implements Comparator<StatsHolder.Key> {
        @Override
        public int compare(StatsHolder.Key k1, StatsHolder.Key k2) {
            assert k1.dimensionValues.size() == k2.dimensionValues.size();
            for (int i = 0; i < k1.dimensionValues.size(); i++) {
                String value1 = k1.dimensionValues.get(i);
                String value2 = k2.dimensionValues.get(i);
                int compareValue = value1.compareTo(value2);
                if (compareValue != 0) {
                    // If the values aren't equal for this dimension, return
                    return compareValue;
                }
            }
            // If all dimension values have been equal, the keys overall are equal
            return 0;
        }
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
