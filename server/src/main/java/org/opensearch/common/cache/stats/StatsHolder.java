/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static org.opensearch.common.settings.Setting.Property.NodeScope;

/**
 * A class caches use to internally keep track of their stats across multiple dimensions. Not intended to be exposed outside the cache.
 */
public class StatsHolder implements Writeable {
    /**
     * For memory purposes, don't track stats for more than this many distinct combinations of dimension values.
     */
    public static final Setting<Integer> MAX_DIMENSION_VALUES_SETTING = Setting.intSetting("cache.stats.max_dimension", 20_000, NodeScope);

    // The list of permitted dimensions.
    private final List<String> dimensionNames;

    /**
     * Determines which combinations of dimension values are tracked separately by this StatsHolder. In every case,
     * incoming keys still must have all dimension values populated.
     */
    public enum TrackingMode {
        /**
         * Tracks stats for each dimension separately. Does not support retrieving stats by combinations of dimension values,
         * only by a single dimension value.
         */
        SEPARATE_DIMENSIONS_ONLY,
        /**
         * Tracks stats for every combination of dimension values. Can retrieve stats for any combination of dimensions,
         * by adding together the combinations.
         */
        ALL_COMBINATIONS,
        /**
         * Tracks stats for a specified subset of combinations. Each combination is kept aggregated in memory. Only stats for
         * the pre-specified combinations can be retrieved.
         */
        SPECIFIC_COMBINATIONS
    }

    // The mode for this instance.
    public final TrackingMode mode;
    // The specific combinations of dimension names to track, if mode is SPECIFIC_COMBINATIONS.
    private final Set<Set<String>> specificCombinations;

    // A map from a set of cache stats dimensions -> stats for that combination of dimensions.
    private final ConcurrentMap<Key, CacheStatsResponse> statsMap;

    int maxDimensionValues;
    CacheStatsResponse totalStats;

    private final Logger logger = LogManager.getLogger(StatsHolder.class);

    public StatsHolder(List<String> dimensionNames, Settings settings, TrackingMode mode) {
        assert (!mode.equals(TrackingMode.SPECIFIC_COMBINATIONS))
            : "Must use constructor specifying specificCombinations when tracking mode is set to SPECIFIC_COMBINATIONS";
        this.dimensionNames = dimensionNames;
        this.statsMap = new ConcurrentHashMap<>();
        this.totalStats = new CacheStatsResponse();
        this.maxDimensionValues = MAX_DIMENSION_VALUES_SETTING.get(settings);
        this.mode = mode;
        this.specificCombinations = new HashSet<>();
    }

    public StatsHolder(List<String> dimensionNames, Settings settings, TrackingMode mode, Set<Set<String>> specificCombinations) {
        if (!mode.equals(TrackingMode.SPECIFIC_COMBINATIONS)) {
            logger.warn("Ignoring specific combinations; tracking mode is not set to SPECIFIC_COMBINATIONS");
        }
        this.dimensionNames = dimensionNames;
        this.statsMap = new ConcurrentHashMap<>();
        this.totalStats = new CacheStatsResponse();
        this.maxDimensionValues = MAX_DIMENSION_VALUES_SETTING.get(settings);
        this.mode = mode;
        for (Set<String> combination : specificCombinations) {
            assert combination.size() > 0 : "Must have at least one dimension name in the combination to record";
        }
        this.specificCombinations = specificCombinations;
    }

    public StatsHolder(StreamInput in) throws IOException {
        this.dimensionNames = List.of(in.readStringArray());
        Map<Key, CacheStatsResponse> readMap = in.readMap(
            i -> new Key(Set.of(i.readArray(CacheStatsDimension::new, CacheStatsDimension[]::new))),
            CacheStatsResponse::new
        );
        this.statsMap = new ConcurrentHashMap<Key, CacheStatsResponse>(readMap);
        this.totalStats = new CacheStatsResponse(in);
        this.maxDimensionValues = in.readVInt();
        this.mode = in.readEnum(TrackingMode.class);
        this.specificCombinations = new HashSet<>();
        int numCombinations = in.readVInt();
        for (int i = 0; i < numCombinations; i++) {
            String[] names = in.readStringArray();
            specificCombinations.add(new HashSet<>(List.of(names)));
        }
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    public ConcurrentMap<Key, CacheStatsResponse> getStatsMap() {
        return statsMap;
    }

    public CacheStatsResponse getTotalStats() {
        return totalStats;
    }

    // For all these increment functions, the dimensions list comes from the key, and contains all dimensions present in dimensionNames.
    public void incrementHitsByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.hits.inc(amount), 1);
    }

    public void incrementMissesByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.misses.inc(amount), 1);
    }

    public void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.evictions.inc(amount), 1);
    }

    public void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes) {
        internalIncrement(dimensions, (response, amount) -> response.memorySize.inc(amount), amountBytes);
    }

    public void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.entries.inc(amount), 1);
    }

    public void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        internalIncrement(dimensions, (response, amount) -> response.entries.inc(amount), -1);
    }

    /**
     * Reset number of entries and memory size when all keys leave the cache, but don't reset hit/miss/eviction numbers
     */
    public void reset() {
        for (Key key : statsMap.keySet()) {
            CacheStatsResponse response = statsMap.get(key);
            response.memorySize.dec(response.getMemorySize());
            response.entries.dec(response.getEntries());
        }
        totalStats.memorySize.dec(totalStats.getMemorySize());
        totalStats.entries.dec(totalStats.getEntries());
    }

    public long count() {
        // Include this here so caches don't have to create an entire CacheStats object to run count().
        return totalStats.getEntries();
    }

    private void internalIncrement(List<CacheStatsDimension> dimensions, BiConsumer<CacheStatsResponse, Long> incrementer, long amount) {
        for (CacheStatsResponse stats : getStatsToIncrement(dimensions)) {
            incrementer.accept(stats, amount);
            incrementer.accept(totalStats, amount);
        }
    }

    private List<CacheStatsResponse> getStatsToIncrement(List<CacheStatsDimension> keyDimensions) {
        List<CacheStatsResponse> result = new ArrayList<>();
        switch (mode) {
            case SEPARATE_DIMENSIONS_ONLY:
                for (CacheStatsDimension dim : keyDimensions) {
                    result.add(internalGetStats(List.of(dim)));
                }
                break;
            case ALL_COMBINATIONS:
                assert keyDimensions.size() == dimensionNames.size();
                result.add(internalGetStats(keyDimensions));
                break;
            case SPECIFIC_COMBINATIONS:
                for (Set<String> combination : specificCombinations) {
                    result.add(internalGetStats(filterDimensionsMatchingCombination(combination, keyDimensions)));
                }
                break;
        }
        return result;
    }

    private List<CacheStatsDimension> filterDimensionsMatchingCombination(
        Set<String> dimCombination,
        List<CacheStatsDimension> dimensions
    ) {
        List<CacheStatsDimension> result = new ArrayList<>();
        for (CacheStatsDimension dim : dimensions) {
            if (dimCombination.contains(dim.dimensionName)) {
                result.add(dim);
            }
        }
        return result;
    }

    Set<Set<String>> getSpecificCombinations() {
        return specificCombinations;
    }

    private CacheStatsResponse internalGetStats(List<CacheStatsDimension> dimensions) {
        CacheStatsResponse response = statsMap.get(new Key(dimensions));
        if (response == null) {
            response = new CacheStatsResponse();
            statsMap.put(new Key(dimensions), response);
            if (statsMap.size() > maxDimensionValues) {
                logger.warn(
                    "Added " + statsMap.size() + "th combination of dimension values to StatsHolder; limit set to " + maxDimensionValues
                );
            }
        }
        return response;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        out.writeMap(
            statsMap,
            (o, key) -> o.writeArray((o1, dim) -> ((CacheStatsDimension) dim).writeTo(o1), key.dimensions.toArray()),
            (o, response) -> response.writeTo(o)
        );
        totalStats.writeTo(out);
        out.writeVInt(maxDimensionValues);
        out.writeEnum(mode);
        // Write Set<Set<String>> as repeated String[]
        out.writeVInt(specificCombinations.size());
        for (Set<String> combination : specificCombinations) {
            out.writeStringArray(combination.toArray(new String[0]));
        }

    }

    /**
     * Unmodifiable wrapper over a set of CacheStatsDimension. Pkg-private for testing.
     */
    public static class Key {
        final Set<CacheStatsDimension> dimensions;

        Key(Set<CacheStatsDimension> dimensions) {
            this.dimensions = Collections.unmodifiableSet(dimensions);
        }

        Key(List<CacheStatsDimension> dimensions) {
            this(new HashSet<>(dimensions));
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (o.getClass() != Key.class) {
                return false;
            }
            Key other = (Key) o;
            return this.dimensions.equals(other.dimensions);
        }

        @Override
        public int hashCode() {
            return this.dimensions.hashCode();
        }
    }
}
