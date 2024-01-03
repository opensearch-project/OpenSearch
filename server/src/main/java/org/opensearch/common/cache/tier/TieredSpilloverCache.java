/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.StoreAwareCacheRemovalNotification;
import org.opensearch.common.cache.store.StoreAwareCacheValue;
import org.opensearch.common.cache.store.builders.StoreAwareCacheBuilder;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.iterable.Iterables;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * This cache spillover the evicted items from heap tier to disk tier. All the new items are first cached on heap
 * and the items evicted from on heap cache are moved to disk based cache. If disk based cache also gets full,
 * then items are eventually evicted from it and removed which will result in cache miss.
 *
 * @param <K> Type of key
 * @param <V> Type of value
 *
 * @opensearch.experimental
 */
public class TieredSpilloverCache<K, V> implements TieredCache<K, V>, StoreAwareCacheEventListener<K, V> {

    // TODO: Remove optional when diskCache implementation is integrated.
    private final Optional<StoreAwareCache<K, V>> onDiskCache;
    private final StoreAwareCache<K, V> onHeapCache;
    private final StoreAwareCacheEventListener<K, V> listener;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    ReleasableLock readLock = new ReleasableLock(readWriteLock.readLock());
    ReleasableLock writeLock = new ReleasableLock(readWriteLock.writeLock());

    /**
     * Maintains caching tiers in ascending order of cache latency.
     */
    private final List<StoreAwareCache<K, V>> cacheList;

    TieredSpilloverCache(Builder<K, V> builder) {
        Objects.requireNonNull(builder.onHeapCacheBuilder, "onHeap cache builder can't be null");
        this.onHeapCache = builder.onHeapCacheBuilder.setEventListener(this).build();
        if (builder.onDiskCacheBuilder != null) {
            this.onDiskCache = Optional.of(builder.onDiskCacheBuilder.setEventListener(this).build());
        } else {
            this.onDiskCache = Optional.empty();
        }
        this.listener = builder.listener;
        this.cacheList = this.onDiskCache.map(diskTier -> Arrays.asList(this.onHeapCache, diskTier)).orElse(List.of(this.onHeapCache));
    }

    // Package private for testing
    StoreAwareCache<K, V> getOnHeapCache() {
        return onHeapCache;
    }

    // Package private for testing
    Optional<StoreAwareCache<K, V>> getOnDiskCache() {
        return onDiskCache;
    }

    @Override
    public V get(K key) {
        StoreAwareCacheValue<V> cacheValue = getValueFromTieredCache(true).apply(key);
        if (cacheValue == null) {
            return null;
        }
        return cacheValue.getValue();
    }

    @Override
    public void put(K key, V value) {
        try (ReleasableLock ignore = writeLock.acquire()) {
            onHeapCache.put(key, value);
            listener.onCached(key, value, CacheStoreType.ON_HEAP);
        }
    }

    @Override
    public V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        // We are skipping calling event listeners at this step as we do another get inside below computeIfAbsent.
        // Where we might end up calling onMiss twice for a key not present in onHeap cache.
        // Similary we might end up calling both onMiss and onHit for a key, in case we are receiving concurrent
        // requests for the same key which requires loading only once.
        StoreAwareCacheValue<V> cacheValue = getValueFromTieredCache(false).apply(key);
        if (cacheValue == null) {
            // Add the value to the onHeap cache. We are calling computeIfAbsent which does another get inside.
            // This is needed as there can be many requests for the same key at the same time and we only want to load
            // the value once.
            V value = null;
            try (ReleasableLock ignore = writeLock.acquire()) {
                value = onHeapCache.computeIfAbsent(key, loader);
            }
            if (loader.isLoaded()) {
                listener.onMiss(key, CacheStoreType.ON_HEAP);
                onDiskCache.ifPresent(diskTier -> listener.onMiss(key, CacheStoreType.DISK));
                listener.onCached(key, value, CacheStoreType.ON_HEAP);
            } else {
                listener.onHit(key, value, CacheStoreType.ON_HEAP);
            }
            return value;
        }
        listener.onHit(key, cacheValue.getValue(), cacheValue.getCacheStoreType());
        if (cacheValue.getCacheStoreType().equals(CacheStoreType.DISK)) {
            listener.onMiss(key, CacheStoreType.ON_HEAP);
        }
        return cacheValue.getValue();
    }

    @Override
    public void invalidate(K key) {
        // We are trying to invalidate the key from all caches though it would be present in only of them.
        // Doing this as we don't know where it is located. We could do a get from both and check that, but what will
        // also trigger a hit/miss listener event, so ignoring it for now.
        try (ReleasableLock ignore = writeLock.acquire()) {
            for (StoreAwareCache<K, V> storeAwareCache : cacheList) {
                storeAwareCache.invalidate(key);
            }
        }
    }

    @Override
    public void invalidateAll() {
        try (ReleasableLock ignore = writeLock.acquire()) {
            for (StoreAwareCache<K, V> storeAwareCache : cacheList) {
                storeAwareCache.invalidateAll();
            }
        }
    }

    /**
     * Provides an iteration over both onHeap and disk keys. This is not protected from any mutations to the cache.
     * @return An iterable over (onHeap + disk) keys
     */
    @Override
    public Iterable<K> keys() {
        Iterable<K> onDiskKeysIterable;
        if (onDiskCache.isPresent()) {
            onDiskKeysIterable = onDiskCache.get().keys();
        } else {
            onDiskKeysIterable = Collections::emptyIterator;
        }
        return Iterables.concat(onHeapCache.keys(), onDiskKeysIterable);
    }

    @Override
    public long count() {
        long totalCount = 0;
        for (StoreAwareCache<K, V> storeAwareCache : cacheList) {
            totalCount += storeAwareCache.count();
        }
        return totalCount;
    }

    @Override
    public void refresh() {
        try (ReleasableLock ignore = writeLock.acquire()) {
            for (StoreAwareCache<K, V> storeAwareCache : cacheList) {
                storeAwareCache.refresh();
            }
        }
    }

    /**
     * Provides an iteration over keys based on desired on cacheStoreType. This is not protected from any mutations
     * to the cache.
     * @param type Type of cacheStoreType
     * @return An iterable over desired CacheStoreType keys
     */
    @Override
    public Iterable<K> cacheKeys(CacheStoreType type) {
        switch (type) {
            case ON_HEAP:
                return onHeapCache.keys();
            case DISK:
                if (onDiskCache.isPresent()) {
                    return onDiskCache.get().keys();
                } else {
                    return Collections::emptyIterator;
                }
            default:
                throw new IllegalArgumentException("Unsupported Cache store type: " + type);
        }
    }

    @Override
    public void refresh(CacheStoreType type) {
        switch (type) {
            case ON_HEAP:
                try (ReleasableLock ignore = writeLock.acquire()) {
                    onHeapCache.refresh();
                }
                break;
            case DISK:
                try (ReleasableLock ignore = writeLock.acquire()) {
                    onDiskCache.ifPresent(ICache::refresh);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported Cache store type: " + type);
        }
    }

    @Override
    public void onMiss(K key, CacheStoreType cacheStoreType) {
        // Misses for tiered cache are tracked here itself.
    }

    @Override
    public void onRemoval(StoreAwareCacheRemovalNotification<K, V> notification) {
        if (RemovalReason.EVICTED.equals(notification.getRemovalReason())
            || RemovalReason.CAPACITY.equals(notification.getRemovalReason())) {
            switch (notification.getCacheStoreType()) {
                case ON_HEAP:
                    try (ReleasableLock ignore = writeLock.acquire()) {
                        onDiskCache.ifPresent(diskTier -> { diskTier.put(notification.getKey(), notification.getValue()); });
                    }
                    onDiskCache.ifPresent(
                        diskTier -> listener.onCached(notification.getKey(), notification.getValue(), CacheStoreType.DISK)
                    );
                    break;
                default:
                    break;
            }
        }
        listener.onRemoval(notification);
    }

    @Override
    public void onHit(K key, V value, CacheStoreType cacheStoreType) {
        // Hits for tiered cache are tracked here itself.
    }

    @Override
    public void onCached(K key, V value, CacheStoreType cacheStoreType) {
        // onCached events for tiered cache are tracked here itself.
    }

    private Function<K, StoreAwareCacheValue<V>> getValueFromTieredCache(boolean triggerEventListener) {
        return key -> {
            try (ReleasableLock ignore = readLock.acquire()) {
                for (StoreAwareCache<K, V> storeAwareCache : cacheList) {
                    V value = storeAwareCache.get(key);
                    if (value != null) {
                        if (triggerEventListener) {
                            listener.onHit(key, value, storeAwareCache.getTierType());
                        }
                        return new StoreAwareCacheValue<>(value, storeAwareCache.getTierType());
                    } else {
                        if (triggerEventListener) {
                            listener.onMiss(key, storeAwareCache.getTierType());
                        }
                    }
                }
            }
            return null;
        };
    }

    /**
     * Builder object for tiered spillover cache.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> {
        private StoreAwareCacheBuilder<K, V> onHeapCacheBuilder;
        private StoreAwareCacheBuilder<K, V> onDiskCacheBuilder;
        private StoreAwareCacheEventListener<K, V> listener;

        public Builder() {}

        public Builder<K, V> setOnHeapCacheBuilder(StoreAwareCacheBuilder<K, V> onHeapCacheBuilder) {
            this.onHeapCacheBuilder = onHeapCacheBuilder;
            return this;
        }

        public Builder<K, V> setOnDiskCacheBuilder(StoreAwareCacheBuilder<K, V> onDiskCacheBuilder) {
            this.onDiskCacheBuilder = onDiskCacheBuilder;
            return this;
        }

        public Builder<K, V> setListener(StoreAwareCacheEventListener<K, V> listener) {
            this.listener = listener;
            return this;
        }

        public TieredSpilloverCache<K, V> build() {
            return new TieredSpilloverCache<>(this);
        }
    }
}
