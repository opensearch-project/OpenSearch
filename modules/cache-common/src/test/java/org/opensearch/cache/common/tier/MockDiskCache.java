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
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockDiskCache<K, V> implements ICache<K, V> {

    Map<K, V> cache;
    int maxSize;
    long delay;

    public MockDiskCache(int maxSize, long delay) {
        this.maxSize = maxSize;
        this.delay = delay;
        this.cache = new ConcurrentHashMap<K, V>();
    }

    @Override
    public V get(K key) {
        V value = cache.get(key);
        return value;
    }

    @Override
    public void put(K key, V value) {
        if (this.cache.size() >= maxSize) { // For simplification
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.cache.put(key, value);
    }

    @Override
    public V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) {
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
    public void invalidate(K key) {
        this.cache.remove(key);
    }

    @Override
    public void invalidateAll() {
        this.cache.clear();
    }

    @Override
    public Iterable<K> keys() {
        return this.cache.keySet();
    }

    @Override
    public long count() {
        return this.cache.size();
    }

    @Override
    public void refresh() {}

    @Override
    public void close() {

    }

    public static class MockDiskCacheFactory implements Factory {

        public static final String NAME = "mockDiskCache";
        final long delay;
        final int maxSize;

        public MockDiskCacheFactory(long delay, int maxSize) {
            this.delay = delay;
            this.maxSize = maxSize;
        }

        @Override
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            return new Builder<K, V>().setMaxSize(maxSize).setDeliberateDelay(delay).build();
        }

        @Override
        public String getCacheName() {
            return NAME;
        }
    }

    public static class Builder<K, V> extends ICacheBuilder<K, V> {

        int maxSize;
        long delay;

        @Override
        public ICache<K, V> build() {
            return new MockDiskCache<K, V>(this.maxSize, this.delay);
        }

        public Builder<K, V> setMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder<K, V> setDeliberateDelay(long millis) {
            this.delay = millis;
            return this;
        }
    }
}
