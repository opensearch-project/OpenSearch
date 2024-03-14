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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * A class caches use to internally keep track of their stats across multiple dimensions. Not intended to be exposed outside the cache.
 */
public class StatsHolder {

    // The list of permitted dimensions. Should be ordered from "outermost" to "innermost", as you would like to
    // aggregate them in an API response.
    private final List<String> dimensionNames;

    // A map from a set of cache stats dimension values -> stats for that ordered list of dimensions.
    private final ConcurrentMap<Key, CacheStatsResponse> statsMap;

    public StatsHolder(List<String> dimensionNames) {
        this.dimensionNames = dimensionNames;
        this.statsMap = new ConcurrentHashMap<>();
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    public ConcurrentMap<Key, CacheStatsResponse> getStatsMap() {
        return statsMap;
    }

    // For all these increment functions, the dimensions list comes from the key, and contains all dimensions present in dimensionNames.
    // The order doesn't have to match the order given in dimensionNames.
    public void incrementHits(ICacheKey<?> key) {
        internalIncrement(key.dimensions, (response, amount) -> response.hits.inc(amount), 1);
    }

    public void incrementMisses(ICacheKey<?> key) {
        internalIncrement(key.dimensions, (response, amount) -> response.misses.inc(amount), 1);
    }

    public void incrementEvictions(ICacheKey<?> key) {
        internalIncrement(key.dimensions, (response, amount) -> response.evictions.inc(amount), 1);
    }

    public void incrementSizeInBytes(ICacheKey<?> key, long amountBytes) {
        internalIncrement(key.dimensions, (response, amount) -> response.sizeInBytes.inc(amount), amountBytes);
    }

    public void incrementEntries(ICacheKey<?> key) {
        internalIncrement(key.dimensions, (response, amount) -> response.entries.inc(amount), 1);
    }

    public void decrementEntries(ICacheKey<?> key) {
        internalIncrement(key.dimensions, (response, amount) -> response.entries.inc(amount), -1);
    }

    /**
     * Reset number of entries and memory size when all keys leave the cache, but don't reset hit/miss/eviction numbers
     */
    public void reset() {
        for (Key key : statsMap.keySet()) {
            CacheStatsResponse response = statsMap.get(key);
            response.sizeInBytes.dec(response.getSizeInBytes());
            response.entries.dec(response.getEntries());
        }
    }

    public long count() {
        // Include this here so caches don't have to create an entire CacheStats object to run count().
        long count = 0L;
        for (Map.Entry<Key, CacheStatsResponse> entry : statsMap.entrySet()) {
            count += entry.getValue().getEntries();
        }
        return count;
    }

    private void internalIncrement(List<CacheStatsDimension> dimensions, BiConsumer<CacheStatsResponse, Long> incrementer, long amount) {
        assert dimensions.size() == dimensionNames.size();
        CacheStatsResponse stats = internalGetStats(dimensions);
        incrementer.accept(stats, amount);
    }

    private CacheStatsResponse internalGetStats(List<CacheStatsDimension> dimensions) {
        Key key = new Key(getOrderedDimensionValues(dimensions, dimensionNames));
        CacheStatsResponse response = statsMap.get(key);
        if (response == null) {
            response = new CacheStatsResponse();
            statsMap.put(key, response);
        }
        return response;
    }

    // Get a list of dimension values, ordered according to dimensionNames, from the possibly differently-ordered dimensions passed in.
    // Static for testing purposes.
    static List<String> getOrderedDimensionValues(List<CacheStatsDimension> dimensions, List<String> dimensionNames) {
        List<String> result = new ArrayList<>();
        for (String dimensionName : dimensionNames) {
            for (CacheStatsDimension dim : dimensions) {
                if (dim.dimensionName.equals(dimensionName)) {
                    result.add(dim.dimensionValue);
                }
            }
        }
        return result;
    }

    public Map<Key, CacheStatsResponse.Snapshot> createSnapshot() {
        ConcurrentHashMap<Key, CacheStatsResponse.Snapshot> snapshot = new ConcurrentHashMap<>();
        for (Map.Entry<Key, CacheStatsResponse> entry : statsMap.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().snapshot());
        }
        // The resulting map is immutable as well as unmodifiable since the backing map is new, not related to statsMap
        return Collections.unmodifiableMap(snapshot);
    }

    /**
     * Unmodifiable wrapper over a list of dimension values, ordered according to dimensionNames. Pkg-private for testing.
     */
    public static class Key {
        final List<String> dimensionValues; // The dimensions must be ordered

        Key(List<String> dimensionValues) {
            this.dimensionValues = Collections.unmodifiableList(dimensionValues);
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
            return this.dimensionValues.equals(other.dimensionValues);
        }

        @Override
        public int hashCode() {
            return this.dimensionValues.hashCode();
        }
    }
}
