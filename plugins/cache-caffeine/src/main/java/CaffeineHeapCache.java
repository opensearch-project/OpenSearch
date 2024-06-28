/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.stats.CacheStatsHolder;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.stats.NoopCacheStatsHolder;
import org.opensearch.common.cache.store.builders.ICacheBuilder;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;


public class CaffeineHeapCache<K,V> implements ICache<K,V> {
    private LoadingCache<ICacheKey<K>, V> cache;
    private final CacheStatsHolder cacheStatsHolder;

    private CaffeineHeapCache(Builder<K, V> builder) {
        List<String> dimensionNames = Objects.requireNonNull(builder.dimensionNames, "Dimension names can't be null");
        if (builder.getStatsTrackingEnabled()) {
            this.cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, "caffeine_heap");
        } else {
            this.cacheStatsHolder = NoopCacheStatsHolder.getInstance();
        }
    }

    @Override
    public V get(ICacheKey<K> key) {
        if (key == null) {
            throw new IllegalArgumentException("Key passed to caffeine heap cache was null.");
        }
        V value;
        value = cache.getIfPresent(key);
        if (value != null) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
        }
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and/or value passed to caffeine heap cache was null.");
        }
        cache.put(key, value);
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) {
        V value;
        Function<ICacheKey<K>, V> mappingFunction = k -> {
            V loadedValue;
            try {
                loadedValue = loader.load(k);
            } catch (Exception ex) {
                throw new OpenSearchException("Exception occurred while getting value from cache loader.");
            }
            return loadedValue;
        };
        value = cache.get(key, mappingFunction);
        if (!loader.isLoaded()) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
        }
        return value;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        if (key == null) {
            throw new IllegalArgumentException("Key passed to caffeine heap cache was null.");
        }
        if (key.getDropStatsForDimensions()) {
            cacheStatsHolder.removeDimensions(key.dimensions);
        }
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
        cacheStatsHolder.reset();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
        ConcurrentMap<ICacheKey<K>,V> map = cache.asMap();
        return map.keySet();
    }

    @Override
    public long count() {
        return cacheStatsHolder.count();
    }

    @Override
    public void refresh() {
        cache.refreshAll(this.keys());
    }

    @Override
    public ImmutableCacheStatsHolder stats(String[] levels) {
        return cacheStatsHolder.getImmutableCacheStatsHolder(levels);
    }

    @Override
    public void close() {}

    public static class Builder<K, V> extends ICacheBuilder<K, V> {
        private List<String> dimensionNames;

        public Builder<K, V> setDimensionNames(List<String> dimensionNames) {
            this.dimensionNames = dimensionNames;
            return this;
        }

        public CaffeineHeapCache<K, V> build() {
            return new CaffeineHeapCache<>(this);
        }
    }



}
