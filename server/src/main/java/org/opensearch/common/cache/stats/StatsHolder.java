/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.cache.ICacheKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * A class caches use to internally keep track of their stats across multiple dimensions.
 * Not intended to be exposed outside the cache; for this, use statsHolder.getCacheStats() to create an immutable
 * copy of the current state of the stats.
 *
 * @opensearch.experimental
 */
public class StatsHolder {

    // The list of permitted dimensions. Should be ordered from "outermost" to "innermost", as you would like to
    // aggregate them in an API response.
    private final List<String> dimensionNames;

    // A map from a set of cache stats dimension values -> stats for that ordered list of dimensions.
    private final ConcurrentMap<Key, CacheStatsCounter> statsMap;

    public StatsHolder(List<String> dimensionNames) {
        this.dimensionNames = dimensionNames;
        this.statsMap = new ConcurrentHashMap<>();
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    ConcurrentMap<Key, CacheStatsCounter> getStatsMap() {
        return statsMap;
    }

    // For all these increment functions, the dimensions list comes from the key, and contains all dimensions present in dimensionNames.
    // The order doesn't have to match the order given in dimensionNames.
    public void incrementHits(ICacheKey<?> key) {
        internalIncrement(key.dimensions, (counter, amount) -> counter.hits.inc(amount), 1);
    }

    public void incrementMisses(ICacheKey<?> key) {
        internalIncrement(key.dimensions, (counter, amount) -> counter.misses.inc(amount), 1);
    }

    public void incrementEvictions(ICacheKey<?> key) {
        internalIncrement(key.dimensions, (counter, amount) -> counter.evictions.inc(amount), 1);
    }

    public void incrementSizeInBytes(ICacheKey<?> key, long amountBytes) {
        internalIncrement(key.dimensions, (counter, amount) -> counter.sizeInBytes.inc(amount), amountBytes);
    }

    public void decrementSizeInBytes(ICacheKey<?> key, long amountBytes) {
        internalDecrement(key.dimensions, (counter, amount) -> counter.sizeInBytes.dec(amount), amountBytes);
    }

    public void incrementEntries(ICacheKey<?> key) {
        internalIncrement(key.dimensions, (counter, amount) -> counter.entries.inc(amount), 1);
    }

    public void decrementEntries(ICacheKey<?> key) {
        internalDecrement(key.dimensions, (counter, amount) -> counter.entries.dec(amount), 1);
    }

    /**
     * Reset number of entries and memory size when all keys leave the cache, but don't reset hit/miss/eviction numbers.
     * This is in line with the behavior of the existing API when caches are cleared.
     */
    public void reset() {
        for (Key key : statsMap.keySet()) {
            CacheStatsCounter counter = statsMap.get(key);
            counter.sizeInBytes.dec(counter.getSizeInBytes());
            counter.entries.dec(counter.getEntries());
        }
    }

    public long count() {
        // Include this here so caches don't have to create an entire CacheStats object to run count().
        long count = 0L;
        for (Map.Entry<Key, CacheStatsCounter> entry : statsMap.entrySet()) {
            count += entry.getValue().getEntries();
        }
        return count;
    }

    /**
     * Use the incrementer function to increment a value in the stats for a set of dimensions. If there is no stats
     * for this set of dimensions, create one.
     */
    private void internalIncrement(List<CacheStatsDimension> dimensions, BiConsumer<CacheStatsCounter, Long> incrementer, long amount) {
        assert dimensions.size() == dimensionNames.size();
        CacheStatsCounter stats = internalGetOrCreateStats(dimensions);
        incrementer.accept(stats, amount);
    }

    /** Similar to internalIncrement, but only applies to existing keys, and does not create a new key if one is absent.
     * This protects us from erroneously decrementing values for keys which have been entirely deleted,
     * for example in an async removal listener.
     */
    private void internalDecrement(List<CacheStatsDimension> dimensions, BiConsumer<CacheStatsCounter, Long> decrementer, long amount) {
        assert dimensions.size() == dimensionNames.size();
        CacheStatsCounter stats = internalGetStats(dimensions);
        if (stats != null) {
            decrementer.accept(stats, amount);
        }
    }

    private CacheStatsCounter internalGetOrCreateStats(List<CacheStatsDimension> dimensions) {
        Key key = getKey(dimensions);
        return statsMap.computeIfAbsent(key, (k) -> new CacheStatsCounter());
    }

    private CacheStatsCounter internalGetStats(List<CacheStatsDimension> dimensions) {
        Key key = getKey(dimensions);
        return statsMap.get(key);
    }

    /**
     * Get a valid key from an unordered list of dimensions.
     */
    private Key getKey(List<CacheStatsDimension> dims) {
        return new Key(getOrderedDimensions(dims, dimensionNames));
    }

    // Get a list of dimension values, ordered according to dimensionNames, from the possibly differently-ordered dimensions passed in.
    // Public and static for testing purposes.
    public static List<CacheStatsDimension> getOrderedDimensions(List<CacheStatsDimension> dimensions, List<String> dimensionNames) {
        List<CacheStatsDimension> result = new ArrayList<>();
        for (String dimensionName : dimensionNames) {
            for (CacheStatsDimension dim : dimensions) {
                if (dim.dimensionName.equals(dimensionName)) {
                    result.add(dim);
                }
            }
        }
        return result;
    }

    /**
     * Produce an immutable CacheStats representation of these stats.
     */
    public CacheStats getCacheStats() {
        Map<Key, CounterSnapshot> snapshot = new HashMap<>();
        for (Map.Entry<Key, CacheStatsCounter> entry : statsMap.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().snapshot());
        }
        // The resulting map is immutable as well as unmodifiable since the backing map is new, not related to statsMap
        Map<Key, CounterSnapshot> immutableSnapshot = Collections.unmodifiableMap(snapshot);
        return new MultiDimensionCacheStats(immutableSnapshot, dimensionNames);
    }

    /**
     * Remove the stats for all keys containing these dimension values.
     */
    public void removeDimensions(List<CacheStatsDimension> dims) {
        Set<Key> keysToRemove = new HashSet<>();
        for (Map.Entry<Key, CacheStatsCounter> entry : statsMap.entrySet()) {
            Key key = entry.getKey();
            if (keyContainsAllDimensions(key, dims)) {
                keysToRemove.add(key);
            }
        }
        for (Key key : keysToRemove) {
            statsMap.remove(key);
        }
    }

    /**
     * Check if the Key contains all the dimensions in dims, matching both dimension name and value.
     */
    boolean keyContainsAllDimensions(Key key, List<CacheStatsDimension> dims) {
        for (CacheStatsDimension dim : dims) {
            int dimensionPosition = dimensionNames.indexOf(dim.dimensionName);
            if (dimensionPosition == -1) {
                throw new IllegalArgumentException("Unrecognized dimension: " + dim.dimensionName + " = " + dim.dimensionValue);
            }
            String keyDimensionValue = key.dimensions.get(dimensionPosition).dimensionValue;
            if (!keyDimensionValue.equals(dim.dimensionValue)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Unmodifiable wrapper over a list of dimension values, ordered according to dimensionNames. Pkg-private for testing.
     */
    public static class Key {
        final List<CacheStatsDimension> dimensions; // The dimensions must be ordered

        public Key(List<CacheStatsDimension> dimensions) {
            this.dimensions = Collections.unmodifiableList(dimensions);
        }

        public List<CacheStatsDimension> getDimensions() {
            return dimensions;
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
