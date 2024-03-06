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
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * A class caches use to internally keep track of their stats across multiple dimensions. Not intended to be exposed outside the cache.
 */
public class StatsHolder implements Writeable {
    /**
     * For memory purposes, don't track stats for more than this many distinct combinations of dimension values.
     */
    public final static int DEFAULT_MAX_DIMENSION_VALUES = 20_000;

    // The list of permitted dimensions.
    private final List<String> dimensionNames;

    // A map from a set of cache stats dimensions -> stats for that combination of dimensions.
    private final ConcurrentMap<Key, CacheStatsResponse> map;

    final int maxDimensionValues;
    CacheStatsResponse totalStats;

    public StatsHolder(List<String> dimensionNames, int maxDimensionValues) {
        this.dimensionNames = dimensionNames;
        this.map = new ConcurrentHashMap<>();
        this.totalStats = new CacheStatsResponse();
        this.maxDimensionValues = maxDimensionValues;
    }

    public StatsHolder(List<String> dimensionNames) {
        this(dimensionNames, DEFAULT_MAX_DIMENSION_VALUES);
    }

    public StatsHolder(StreamInput in) throws IOException {
        this.dimensionNames = List.of(in.readStringArray());
        Map<Key, CacheStatsResponse> readMap = in.readMap(
            i -> new Key(Set.of(i.readArray(CacheStatsDimension::new, CacheStatsDimension[]::new))),
            CacheStatsResponse::new
        );
        this.map = new ConcurrentHashMap<Key, CacheStatsResponse>(readMap);
        this.totalStats = new CacheStatsResponse(in);
        this.maxDimensionValues = in.readVInt();
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    public ConcurrentMap<Key, CacheStatsResponse> getMap() {
        return map;
    }

    public CacheStatsResponse getTotalStats() {
        return totalStats;
    }

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
        for (Key key : map.keySet()) {
            CacheStatsResponse response = map.get(key);
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
        CacheStatsResponse stats = internalGetStats(dimensions);
        incrementer.accept(stats, amount);
        incrementer.accept(totalStats, amount);
    }

    private CacheStatsResponse internalGetStats(List<CacheStatsDimension> dimensions) {
        assert dimensions.size() == dimensionNames.size();
        CacheStatsResponse response = map.get(new Key(dimensions));
        if (response == null) {
            if (map.size() < maxDimensionValues) {
                response = new CacheStatsResponse();
                map.put(new Key(dimensions), response);
            } else {
                throw new RuntimeException("Cannot add new combination of dimension values to stats object; reached maximum");
            }
        }
        return response;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        out.writeMap(
            map,
            (o, key) -> o.writeArray((o1, dim) -> ((CacheStatsDimension) dim).writeTo(o1), key.dimensions.toArray()),
            (o, response) -> response.writeTo(o)
        );
        totalStats.writeTo(out);
        out.writeVInt(maxDimensionValues);
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
