/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.store.Cache;
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.StoreAwareCacheRemovalNotification;
import org.opensearch.common.cache.store.StoreAwareCacheValue;
import org.opensearch.common.cache.store.builders.StoreAwareCacheBuilder;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.EventType;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListenerConfiguration;
import org.opensearch.common.cache.store.listeners.dispatchers.StoreAwareCacheListenerDispatcherDefaultImpl;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * This cache spillover the evicted items from upper tier to lower tier. For now, we are spilling the in-memory
 * cache items to disk tier cache. All the new items are cached onHeap and if any items are evicted are moved to disk
 * cache.
 * @param <K> Type of key
 * @param <V> Type of value
 */
public class TieredSpilloverCache<K, V> implements TieredCache<K, V>, StoreAwareCacheEventListener<K, V> {

    private final Optional<StoreAwareCache<K, V>> onDiskCache;
    private final StoreAwareCache<K, V> onHeapCache;
    private final StoreAwareCacheListenerDispatcherDefaultImpl<K, V> eventDispatcher;

    /**
     * Maintains caching tiers in order of get calls.
     */
    private final List<StoreAwareCache<K, V>> cacheList;

    TieredSpilloverCache(Builder<K, V> builder) {
        Objects.requireNonNull(builder.onHeapCacheBuilder, "onHeap cache builder can't be null");
        this.onHeapCache = builder.onHeapCacheBuilder.setEventListenerConfiguration(getCacheEventListenerConfiguration()).build();
        if (builder.onDiskCacheBuilder != null) {
            this.onDiskCache = Optional.of(
                builder.onDiskCacheBuilder.setEventListenerConfiguration(getCacheEventListenerConfiguration()).build()
            );
        } else {
            this.onDiskCache = Optional.empty();
        }
        this.eventDispatcher = new StoreAwareCacheListenerDispatcherDefaultImpl<K, V>(builder.listenerConfiguration);
        this.cacheList = this.onDiskCache.map(diskTier -> Arrays.asList(this.onHeapCache, diskTier)).orElse(List.of(this.onHeapCache));
    }

    private StoreAwareCacheEventListenerConfiguration<K, V> getCacheEventListenerConfiguration() {
        return new StoreAwareCacheEventListenerConfiguration.Builder<K, V>().setEventListener(this)
            .setEventTypes(EnumSet.of(EventType.ON_REMOVAL, EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS))
            .build();
    }

    @Override
    public V get(K key) {
        StoreAwareCacheValue<V> cacheValue = getValueFromTieredCache().apply(key);
        if (cacheValue == null) {
            return null;
        }
        return cacheValue.getValue();
    }

    @Override
    public void put(K key, V value) {
        onHeapCache.put(key, value);
    }

    @Override
    public V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        StoreAwareCacheValue<V> cacheValue = getValueFromTieredCache().apply(key);
        if (cacheValue == null) {
            // Add the value to the onHeap cache. Any items if evicted will be moved to lower tier.
            V value = onHeapCache.compute(key, loader);
            return value;
        }
        return cacheValue.getValue();
    }

    @Override
    public void invalidate(K key) {
        // We are trying to invalidate the key from all caches though it would be present in only of them.
        // Doing this as we don't where it is located. We could do a get from both and check that, but what will also
        // trigger a hit/miss listener event, so ignoring it for now.
        for (StoreAwareCache<K, V> storeAwareCache : cacheList) {
            storeAwareCache.invalidate(key);
        }
    }

    @Override
    public V compute(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        return onHeapCache.compute(key, loader);
    }

    @Override
    public void invalidateAll() {
        for (StoreAwareCache<K, V> storeAwareCache : cacheList) {
            storeAwareCache.invalidateAll();
        }
    }

    @Override
    public Iterable<K> keys() {
        Iterable<K> onDiskKeysIterable;
        if (onDiskCache.isPresent()) {
            onDiskKeysIterable = onDiskCache.get().keys();
        } else {
            onDiskKeysIterable = Collections::emptyIterator;
        }
        return new MergedIterable<>(onHeapCache.keys(), onDiskKeysIterable);
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
        for (StoreAwareCache<K, V> storeAwareCache : cacheList) {
            storeAwareCache.refresh();
        }
    }

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
                onHeapCache.refresh();
                break;
            case DISK:
                onDiskCache.ifPresent(Cache::refresh);
                break;
            default:
                throw new IllegalArgumentException("Unsupported Cache store type: " + type);
        }
    }

    @Override
    public void onMiss(K key, CacheStoreType cacheStoreType) {
        eventDispatcher.dispatch(key, null, cacheStoreType, EventType.ON_MISS);
    }

    @Override
    public void onRemoval(StoreAwareCacheRemovalNotification<K, V> notification) {
        if (RemovalReason.EVICTED.equals(notification.getRemovalReason())) {
            switch (notification.getCacheStoreType()) {
                case ON_HEAP:
                    onDiskCache.ifPresent(diskTier -> { diskTier.put(notification.getKey(), notification.getValue()); });
                    break;
                default:
                    break;
            }
        }
        eventDispatcher.dispatchRemovalEvent(notification);
    }

    @Override
    public void onHit(K key, V value, CacheStoreType cacheStoreType) {
        eventDispatcher.dispatch(key, value, cacheStoreType, EventType.ON_HIT);
    }

    @Override
    public void onCached(K key, V value, CacheStoreType cacheStoreType) {
        eventDispatcher.dispatch(key, value, cacheStoreType, EventType.ON_CACHED);
    }

    private Function<K, StoreAwareCacheValue<V>> getValueFromTieredCache() {
        return key -> {
            for (StoreAwareCache<K, V> storeAwareCache : cacheList) {
                V value = storeAwareCache.get(key);
                if (value != null) {
                    return new StoreAwareCacheValue<>(value, storeAwareCache.getTierType());
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
        private StoreAwareCacheEventListenerConfiguration<K, V> listenerConfiguration;

        public Builder() {}

        public Builder<K, V> setOnHeapCacheBuilder(StoreAwareCacheBuilder<K, V> onHeapCacheBuilder) {
            this.onHeapCacheBuilder = onHeapCacheBuilder;
            return this;
        }

        public Builder<K, V> setOnDiskCacheBuilder(StoreAwareCacheBuilder<K, V> onDiskCacheBuilder) {
            this.onDiskCacheBuilder = onDiskCacheBuilder;
            return this;
        }

        public Builder<K, V> setListenerConfiguration(StoreAwareCacheEventListenerConfiguration<K, V> listenerConfiguration) {
            this.listenerConfiguration = listenerConfiguration;
            return this;
        }

        public TieredSpilloverCache<K, V> build() {
            return new TieredSpilloverCache<>(this);
        }
    }

    /**
     * Returns a merged iterable which can be used to iterate over both onHeap and disk cache keys.
     * @param <K> Type of key.
     */
    public class MergedIterable<K> implements Iterable<K> {

        private final Iterable<K> onHeapKeysIterable;
        private final Iterable<K> onDiskKeysIterable;

        public MergedIterable(Iterable<K> onHeapKeysIterable, Iterable<K> onDiskKeysIterable) {
            this.onHeapKeysIterable = onHeapKeysIterable;
            this.onDiskKeysIterable = onDiskKeysIterable;
        }

        @Override
        public Iterator<K> iterator() {
            return new Iterator<K>() {
                private final Iterator<K> onHeapIterator = onHeapKeysIterable.iterator();
                private final Iterator<K> onDiskIterator = onDiskKeysIterable.iterator();
                private boolean useOnHeapIterator = true;

                @Override
                public boolean hasNext() {
                    return onHeapIterator.hasNext() || onDiskIterator.hasNext();
                }

                @Override
                public K next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }

                    if (useOnHeapIterator && onHeapIterator.hasNext()) {
                        return onHeapIterator.next();
                    } else {
                        useOnHeapIterator = false;
                        return onDiskIterator.next();
                    }
                }
            };
        }
    }
}
