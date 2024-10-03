/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.cache.stats.CacheStatsHolder;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.stats.NoopCacheStatsHolder;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class MockDiskCache<K, V> implements ICache<K, V> {

    Map<ICacheKey<K>, V> cache;
    int maxSize;
    long delay;

    private final RemovalListener<ICacheKey<K>, V> removalListener;
    private final CacheStatsHolder statsHolder; // Only update for number of entries; this is only used to test statsTrackingEnabled logic
                                                // in TSC

    public MockDiskCache(int maxSize, long delay, RemovalListener<ICacheKey<K>, V> removalListener, boolean statsTrackingEnabled) {
        this.maxSize = maxSize;
        this.delay = delay;
        this.removalListener = removalListener;
        this.cache = new ConcurrentHashMap<ICacheKey<K>, V>();
        if (statsTrackingEnabled) {
            this.statsHolder = new DefaultCacheStatsHolder(List.of(), "mock_disk_cache");
        } else {
            this.statsHolder = NoopCacheStatsHolder.getInstance();
        }
    }

    @Override
    public V get(ICacheKey<K> key) {
        V value = cache.get(key);
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        if (this.cache.size() >= maxSize) { // For simplification
            this.removalListener.onRemoval(new RemovalNotification<>(key, value, RemovalReason.EVICTED));
            this.statsHolder.decrementItems(List.of());
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.cache.put(key, value);
        this.statsHolder.incrementItems(List.of());
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) {
        V value = cache.computeIfAbsent(key, key1 -> {
            try {
                return loader.load(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return value;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        V value = this.cache.remove(key);
        if (value != null) {
            removalListener.onRemoval(new RemovalNotification<>(key, cache.get(key), RemovalReason.INVALIDATED));
        }
    }

    @Override
    public void invalidateAll() {
        this.cache.clear();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
        return () -> new CacheKeyIterator<>(cache, removalListener);
    }

    @Override
    public long count() {
        return this.cache.size();
    }

    @Override
    public void refresh() {}

    @Override
    public ImmutableCacheStatsHolder stats() {
        // To allow testing of statsTrackingEnabled logic in TSC, return a dummy ImmutableCacheStatsHolder with the
        // right number of entries, unless statsTrackingEnabled is false
        return statsHolder.getImmutableCacheStatsHolder(null);
    }

    @Override
    public ImmutableCacheStatsHolder stats(String[] levels) {
        return null;
    }

    @Override
    public void close() {

    }

    public static class MockDiskCacheFactory implements Factory {

        public static final String NAME = "mockDiskCache";
        final long delay;
        final int maxSize;
        final boolean statsTrackingEnabled;

        public MockDiskCacheFactory(long delay, int maxSize, boolean statsTrackingEnabled) {
            this.delay = delay;
            this.maxSize = maxSize;
            this.statsTrackingEnabled = statsTrackingEnabled;
        }

        @Override
        @SuppressWarnings({ "unchecked" })
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            // As we can't directly IT with the tiered cache and ehcache, check that we receive non-null serializers, as an ehcache disk
            // cache would require.
            assert config.getKeySerializer() != null;
            assert config.getValueSerializer() != null;
            MockDiskCache.Builder<K, V> builder = (Builder<K, V>) new Builder<K, V>().setKeySerializer(
                (Serializer<K, byte[]>) config.getKeySerializer()
            )
                .setValueSerializer((Serializer<V, byte[]>) config.getValueSerializer())
                .setDeliberateDelay(delay)
                .setRemovalListener(config.getRemovalListener())
                .setStatsTrackingEnabled(config.getStatsTrackingEnabled());
            if (config.getSegmentCount() > 0) {
                int perSegmentSize = maxSize / config.getSegmentCount();
                if (perSegmentSize <= 0) {
                    throw new IllegalArgumentException("Per segment size for mock disk cache should be " + "greater than 0");
                }
                builder.setMaxSize(perSegmentSize);
            } else {
                builder.setMaxSize(maxSize);
            }
            return builder.build();
        }

        @Override
        public String getCacheName() {
            return NAME;
        }
    }

    public static class Builder<K, V> extends ICacheBuilder<K, V> {

        int maxSize;
        long delay;
        Serializer<K, byte[]> keySerializer;
        Serializer<V, byte[]> valueSerializer;

        @Override
        public ICache<K, V> build() {
            return new MockDiskCache<K, V>(this.maxSize, this.delay, this.getRemovalListener(), getStatsTrackingEnabled());
        }

        public Builder<K, V> setMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder<K, V> setDeliberateDelay(long millis) {
            this.delay = millis;
            return this;
        }

        public Builder<K, V> setKeySerializer(Serializer<K, byte[]> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder<K, V> setValueSerializer(Serializer<V, byte[]> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

    }

    /**
     * Provides a iterator over keys.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    static class CacheKeyIterator<K, V> implements Iterator<K> {
        private final Iterator<Map.Entry<K, V>> entryIterator;
        private final Map<K, V> cache;
        private final RemovalListener<K, V> removalListener;
        private K currentKey;

        public CacheKeyIterator(Map<K, V> cache, RemovalListener<K, V> removalListener) {
            this.entryIterator = cache.entrySet().iterator();
            this.removalListener = removalListener;
            this.cache = cache;
        }

        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public K next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map.Entry<K, V> entry = entryIterator.next();
            currentKey = entry.getKey();
            return currentKey;
        }

        @Override
        public void remove() {
            if (currentKey == null) {
                throw new IllegalStateException("No element to remove");
            }
            V value = cache.get(currentKey);
            cache.remove(currentKey);
            this.removalListener.onRemoval(new RemovalNotification<>(currentKey, value, RemovalReason.INVALIDATED));
            currentKey = null;
        }
    }
}
