/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.*;
import org.opensearch.common.cache.stats.CacheStatsHolder;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.stats.NoopCacheStatsHolder;
import org.opensearch.common.cache.store.builders.ICacheBuilder;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.ToLongBiFunction;


public class CaffeineHeapCache<K,V> implements ICache<K,V> {

    private final LoadingCache<ICacheKey<K>, V> cache;
    private final CacheStatsHolder cacheStatsHolder;
    private final ToLongBiFunction<ICacheKey<K>, V> weigher;
    private final CaffeineRemovalListener caffeineRemovalListener;

    private CaffeineHeapCache(Builder<K, V> builder) {
        List<String> dimensionNames = Objects.requireNonNull(builder.dimensionNames, "Dimension names can't be null");
        if (builder.getStatsTrackingEnabled()) {
            this.cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, "caffeine_heap");
        } else {
            this.cacheStatsHolder = NoopCacheStatsHolder.getInstance();
        }
        Objects.requireNonNull(builder.getWeigher(), "Weigher can't be null");
        this.weigher = builder.getWeigher();
        Objects.requireNonNull(builder.getRemovalListener(), "Removal listener can't be null");
        this.caffeineRemovalListener = new CaffeineRemovalListener(builder.getRemovalListener());

        cache = Caffeine.newBuilder()
            .removalListener(this.caffeineRemovalListener)
            .maximumWeight(builder.getMaxWeightInBytes())
            .expireAfterAccess(builder.getExpireAfterAcess().duration(), builder.getExpireAfterAcess().timeUnit())
            .weigher(new CaffeineWeigher(builder.getWeigher()))
            .executor(builder.getExecutor())
            .build(k -> null);
    }

    /**
     * Wrapper over ICache weigher to be used by Caffeine
     */
    private class CaffeineWeigher implements Weigher<ICacheKey<K>, V> {
        private final ToLongBiFunction<ICacheKey<K>, V> weigher;

        private CaffeineWeigher(ToLongBiFunction<ICacheKey<K>, V> weigher) {
            this.weigher = weigher;
        }

        @Override
        public int weigh(ICacheKey<K> key, V value) {
            return (int) this.weigher.applyAsLong(key, value);
        }
    }

    private class CaffeineRemovalListener implements RemovalListener<ICacheKey<K>, V> {
        private final org.opensearch.common.cache.RemovalListener<ICacheKey<K>, V> removalListener;

        CaffeineRemovalListener(org.opensearch.common.cache.RemovalListener<ICacheKey<K>, V> removalListener) {
            this.removalListener = removalListener;
        }

        @Override
        public void onRemoval(ICacheKey<K> key, V value, RemovalCause removalCause) {
            switch (removalCause) {
                case SIZE:
                    removalListener.onRemoval(
                        new RemovalNotification<>(key, value, RemovalReason.CAPACITY)
                    );
                    cacheStatsHolder.incrementEvictions(key.dimensions);
                    break;
                case EXPIRED:
                    removalListener.onRemoval(
                        new RemovalNotification<>(key, value, RemovalReason.INVALIDATED)
                    );
                    cacheStatsHolder.incrementEvictions(key.dimensions);
                    break;
                case EXPLICIT:
                    removalListener.onRemoval(
                        new RemovalNotification<>(key, value, RemovalReason.EXPLICIT)
                    );
                    break;
                case REPLACED:
                    removalListener.onRemoval(
                        new RemovalNotification<>(key, value, RemovalReason.REPLACED)
                    );
                    break;
            }
            cacheStatsHolder.decrementItems(key.dimensions);
            cacheStatsHolder.decrementSizeInBytes(key.dimensions, weigher.applyAsLong(key, value));
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
        cacheStatsHolder.incrementItems(key.dimensions);
        cacheStatsHolder.incrementSizeInBytes(key.dimensions, weigher.applyAsLong(key, value));
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
            cacheStatsHolder.incrementItems(key.dimensions);
            cacheStatsHolder.incrementSizeInBytes(key.dimensions, weigher.applyAsLong(key, value));
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
        V value = cache.get(key);
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
        private Executor executor = ForkJoinPool.commonPool();

        public Builder<K, V> setDimensionNames(List<String> dimensionNames) {
            this.dimensionNames = dimensionNames;
            return this;
        }

        public Builder<K, V> setExecutor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Executor getExecutor() {
            return executor;
        }

        public CaffeineHeapCache<K, V> build() {
            return new CaffeineHeapCache<>(this);
        }
    }

    /**
     * Manually performs Caffeine maintenance cycle, which includes removing expired entries from the cache.
     * Used for testing.
     */
    void cleanUp() {
        cache.cleanUp();
    }
}
