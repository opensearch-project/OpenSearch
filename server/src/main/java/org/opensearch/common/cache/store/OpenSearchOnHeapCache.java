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
import org.opensearch.common.cache.store.listeners.EventType;
import org.opensearch.common.cache.store.listeners.dispatchers.StoreAwareCacheEventListenerDispatcher;
import org.opensearch.common.cache.store.listeners.dispatchers.StoreAwareCacheListenerDispatcherDefaultImpl;

/**
 * This variant of on-heap cache uses OpenSearch custom cache implementation.
 * @param <K> Type of key.
 * @param <V> Type of value.
 */
public class OpenSearchOnHeapCache<K, V> implements StoreAwareCache<K, V>, RemovalListener<K, V> {

    private final Cache<K, V> cache;

    private final StoreAwareCacheEventListenerDispatcher<K, V> eventDispatcher;

    public OpenSearchOnHeapCache(Builder<K, V> builder) {
        CacheBuilder<K, V> cacheBuilder = CacheBuilder.<K, V>builder()
            .setMaximumWeight(builder.getMaxWeightInBytes())
            .weigher(builder.getWeigher())
            .removalListener(this);
        if (builder.getExpireAfterAcess() != null) {
            cacheBuilder.setExpireAfterAccess(builder.getExpireAfterAcess());
        }
        cache = cacheBuilder.build();
        this.eventDispatcher = new StoreAwareCacheListenerDispatcherDefaultImpl<>(builder.getListenerConfiguration());
    }

    @Override
    public V get(K key) {
        V value = cache.get(key);
        if (value != null) {
            eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_HIT);
        } else {
            eventDispatcher.dispatch(key, null, CacheStoreType.ON_HEAP, EventType.ON_MISS);
        }
        return value;
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
        eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_CACHED);
    }

    @Override
    public V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        V value = cache.computeIfAbsent(key, key1 -> loader.load(key));
        if (!loader.isLoaded()) {
            eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_HIT);
        } else {
            eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_MISS);
            eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_CACHED);
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
        eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_CACHED);
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
        eventDispatcher.dispatchRemovalEvent(
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
