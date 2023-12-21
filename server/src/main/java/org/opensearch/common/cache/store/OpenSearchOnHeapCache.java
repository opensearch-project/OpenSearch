/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.store.builders.StoreAwareCacheBuilder;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;

/**
 * This variant of on-heap cache uses OpenSearch custom cache implementation.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.experimental
 */
public class OpenSearchOnHeapCache<K, V> implements StoreAwareCache<K, V>, RemovalListener<K, V> {

    private final Cache<K, V> cache;

    private final StoreAwareCacheEventListener<K, V> eventListener;

    public OpenSearchOnHeapCache(Builder<K, V> builder) {
        CacheBuilder<K, V> cacheBuilder = CacheBuilder.<K, V>builder()
            .setMaximumWeight(builder.getMaxWeightInBytes())
            .weigher(builder.getWeigher())
            .removalListener(this);
        if (builder.getExpireAfterAcess() != null) {
            cacheBuilder.setExpireAfterAccess(builder.getExpireAfterAcess());
        }
        cache = cacheBuilder.build();
        this.eventListener = builder.getEventListener();
    }

    @Override
    public V get(K key) {
        V value = cache.get(key);
        if (value != null) {
            eventListener.onHit(key, value, CacheStoreType.ON_HEAP);
        } else {
            eventListener.onMiss(key, CacheStoreType.ON_HEAP);
        }
        return value;
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
        eventListener.onCached(key, value, CacheStoreType.ON_HEAP);
    }

    @Override
    public V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        V value = cache.computeIfAbsent(key, key1 -> loader.load(key));
        if (!loader.isLoaded()) {
            eventListener.onHit(key, value, CacheStoreType.ON_HEAP);
        } else {
            eventListener.onMiss(key, CacheStoreType.ON_HEAP);
            eventListener.onCached(key, value, CacheStoreType.ON_HEAP);
        }
        return value;
    }

    @Override
    public void invalidate(K key) {
        cache.invalidate(key);
    }

    @Override
    public V compute(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        V value = cache.compute(key, key1 -> loader.load(key));
        eventListener.onCached(key, value, CacheStoreType.ON_HEAP);
        return value;
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public Iterable<K> keys() {
        return cache.keys();
    }

    @Override
    public long count() {
        return cache.count();
    }

    @Override
    public void refresh() {
        cache.refresh();
    }

    @Override
    public CacheStoreType getTierType() {
        return CacheStoreType.ON_HEAP;
    }

    @Override
    public void onRemoval(RemovalNotification<K, V> notification) {
        eventListener.onRemoval(
            new StoreAwareCacheRemovalNotification<>(
                notification.getKey(),
                notification.getValue(),
                notification.getRemovalReason(),
                CacheStoreType.ON_HEAP
            )
        );
    }

    /**
     * Builder object
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> extends StoreAwareCacheBuilder<K, V> {

        @Override
        public StoreAwareCache<K, V> build() {
            return new OpenSearchOnHeapCache<K, V>(this);
        }
    }
}
