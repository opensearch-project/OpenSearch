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
import java.util.concurrent.ConcurrentHashMap;

/**
 * A CacheStats object supporting aggregation over multiple different dimensions.
 * Stores a fixed snapshot of a cache's stats; does not allow changes.
 */
public class MultiDimensionCacheStats implements CacheStats {
    // A snapshot of a StatsHolder containing stats maintained by the cache.
    // Pkg-private for testing.
    final Map<StatsHolder.Key, CacheStatsCounter.Snapshot> snapshot;
    final List<String> dimensionNames;

    public MultiDimensionCacheStats(Map<StatsHolder.Key, CacheStatsCounter.Snapshot> snapshot, List<String> dimensionNames) {
        this.snapshot = snapshot;
        this.dimensionNames = dimensionNames;
    }

    public MultiDimensionCacheStats(StreamInput in) throws IOException {
        this.dimensionNames = List.of(in.readStringArray());
        Map<StatsHolder.Key, CacheStatsCounter.Snapshot> readMap = in.readMap(
            i -> new StatsHolder.Key(List.of(i.readArray(StreamInput::readString, String[]::new))),
            CacheStatsCounter.Snapshot::new
        );
        this.snapshot = new ConcurrentHashMap<StatsHolder.Key, CacheStatsCounter.Snapshot>(readMap);
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
    public CacheStatsCounter.Snapshot getTotalStats() {
        CacheStatsCounter counter = new CacheStatsCounter();
        // To avoid making many Snapshot objects for the incremental sums, add to a mutable CacheStatsCounter and finally convert to
        // Snapshot
        for (Map.Entry<StatsHolder.Key, CacheStatsCounter.Snapshot> entry : snapshot.entrySet()) {
            counter.add(entry.getValue());
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
     * values are grouped by their dimension values.
     * @param levels The levels to aggregate by
     * @return The resulting stats
     */
    public TreeMap<StatsHolder.Key, CacheStatsCounter.Snapshot> aggregateByLevels(List<String> levels) {
        if (levels.size() == 0) {
            throw new IllegalArgumentException("Levels cannot have size 0");
        }
        int[] levelIndices = getLevelIndices(levels);
        TreeMap<StatsHolder.Key, CacheStatsCounter.Snapshot> result = new TreeMap<>(new KeyComparator());

        for (Map.Entry<StatsHolder.Key, CacheStatsCounter.Snapshot> entry : snapshot.entrySet()) {
            List<String> levelValues = new ArrayList<>(); // The values for the dimensions we're aggregating over for this key
            for (int levelIndex : levelIndices) {
                levelValues.add(entry.getKey().dimensionValues.get(levelIndex));
            }
            // The new key for the aggregated stats contains only the dimensions specified in levels
            StatsHolder.Key levelsKey = new StatsHolder.Key(levelValues);
            CacheStatsCounter.Snapshot originalCounter = entry.getValue();
            if (result.containsKey(levelsKey)) {
                result.put(levelsKey, result.get(levelsKey).add(originalCounter));
            } else {
                result.put(levelsKey, originalCounter);
            }
        }
        return result;
    }

    // First compare outermost dimension, then second outermost, etc.
    // Pkg-private for testing
    static class KeyComparator implements Comparator<StatsHolder.Key> {
        @Override
        public int compare(StatsHolder.Key k1, StatsHolder.Key k2) {
            assert k1.dimensionValues.size() == k2.dimensionValues.size();
            for (int i = 0; i < k1.dimensionValues.size(); i++) {
                int compareValue = k1.dimensionValues.get(i).compareTo(k2.dimensionValues.get(i));
                if (compareValue != 0) {
                    return compareValue;
                }
            }
            return 0;
        }
    }

    private int[] getLevelIndices(List<String> levels) {
        // Levels must all be present in dimensionNames and also be in matching order
        // Return a list of indices in dimensionNames corresponding to each level
        int[] result = new int[levels.size()];
        int levelsIndex = 0;

        for (int namesIndex = 0; namesIndex < dimensionNames.size(); namesIndex++) {
            if (dimensionNames.get(namesIndex).equals(levels.get(levelsIndex))) {
                result[levelsIndex] = namesIndex;
                levelsIndex++;
            }
            if (levelsIndex >= levels.size()) {
                break;
            }
        }
        if (levelsIndex != levels.size()) {
            throw new IllegalArgumentException("Invalid levels: " + levels);
        }
        return result;
    }

    // TODO (in API PR): Produce XContent based on aggregateByLevels()
}
