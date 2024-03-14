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
 */
public class MultiDimensionCacheStats implements CacheStats {
    // A StatsHolder containing stats maintained by the cache.
    // Pkg-private for testing.
    final StatsHolder statsHolder;

    public MultiDimensionCacheStats(StatsHolder statsHolder) {
        this.statsHolder = statsHolder;
    }

    public MultiDimensionCacheStats(StreamInput in) throws IOException {
        this.statsHolder = new StatsHolder(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        statsHolder.writeTo(out);
    }

    @Override
    public CacheStatsResponse getTotalStats() {
        CacheStatsResponse response = new CacheStatsResponse();
        response.add(statsHolder.getTotalStats()); // Return a copy to prevent consumers of this method from changing the original
        return response;
    }

    @Override
    public long getTotalHits() {
        return statsHolder.getTotalStats().getHits();
    }

    @Override
    public long getTotalMisses() {
        return statsHolder.getTotalStats().getMisses();
    }

    @Override
    public long getTotalEvictions() {
        return statsHolder.getTotalStats().getEvictions();
    }

    @Override
    public long getTotalSizeInBytes() {
        return statsHolder.getTotalStats().getSizeInBytes();
    }

    @Override
    public long getTotalEntries() {
        return statsHolder.getTotalStats().getEntries();
    }

    /**
     * Return a TreeMap containing stats values aggregated by the levels passed in. Results are ordered so that
     * values are grouped by their dimension values.
     * @param levels The levels to aggregate by
     * @return The resulting stats
     */
    public TreeMap<StatsHolder.Key, CacheStatsResponse> aggregateByLevels(List<String> levels) {
        if (levels.size() == 0) {
            throw new IllegalArgumentException("Levels cannot have size 0");
        }
        int[] levelIndices = getLevelIndices(levels);
        TreeMap<StatsHolder.Key, CacheStatsResponse> result = new TreeMap<>(new KeyComparator());

        Map<StatsHolder.Key, CacheStatsResponse> map = statsHolder.getStatsMap();
        for (Map.Entry<StatsHolder.Key, CacheStatsResponse> entry : map.entrySet()) {
            List<String> levelValues = new ArrayList<>(); // The values for the dimensions we're aggregating over for this key
            for (int levelIndex : levelIndices) {
                levelValues.add(entry.getKey().dimensionValues.get(levelIndex));
            }
            // The new key for the aggregated stats contains only the dimensions specified in levels
            StatsHolder.Key levelsKey = new StatsHolder.Key(levelValues);
            CacheStatsResponse originalResponse = entry.getValue();
            if (result.containsKey(levelsKey)) {
                result.get(levelsKey).add(originalResponse);
            } else {
                CacheStatsResponse newResponse = new CacheStatsResponse();
                newResponse.add(originalResponse);
                result.put(levelsKey, newResponse); // add a copy, not the original
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

        for (int namesIndex = 0; namesIndex < statsHolder.getDimensionNames().size(); namesIndex++) {
            if (statsHolder.getDimensionNames().get(namesIndex).equals(levels.get(levelsIndex))) {
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
