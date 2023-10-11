/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 *
 * @param <K>
 * @param <V>
 */
public class TieredCacheSpilloverStrategyHandler<K, V> implements TieredCacheHandler<K, V>, RemovalListener<K, V> {

    private final OnHeapCachingTier<K, V> onHeapCachingTier;
    private final CachingTier<K, V> diskCachingTier;
    private final TieredCacheEventListener<K, V> tieredCacheEventListener;

    /**
     * Maintains caching tiers in order of get calls.
     */
    private final List<CachingTier<K, V>> cachingTierList;

    private TieredCacheSpilloverStrategyHandler(
        OnHeapCachingTier<K, V> onHeapCachingTier,
        CachingTier<K, V> diskCachingTier,
        TieredCacheEventListener<K, V> tieredCacheEventListener
    ) {
        this.onHeapCachingTier = Objects.requireNonNull(onHeapCachingTier);
        this.diskCachingTier = Objects.requireNonNull(diskCachingTier);
        this.tieredCacheEventListener = tieredCacheEventListener;
        this.cachingTierList = Arrays.asList(onHeapCachingTier, diskCachingTier);
        setRemovalListeners();
    }

    @Override
    public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
        CacheValue<V> cacheValue = getValueFromTierCache().apply(key);
        if (cacheValue == null) {
            // Add the value to the onHeap cache. Any items if evicted will be moved to lower tier
            V value = onHeapCachingTier.compute(key, loader);
            tieredCacheEventListener.onCached(key, value, TierType.ON_HEAP);
            return value;
        } else {
            //tieredCacheEventListener.onHit(key, cacheValue.value, cacheValue.source); // this double counts, see line 122
        }
        return cacheValue.value;
    }

    @Override
    public V get(K key) {
        CacheValue<V> cacheValue = getValueFromTierCache().apply(key);
        if (cacheValue == null) {
            return null;
        }
        return cacheValue.value;
    }

    @Override
    public void invalidate(K key) {
        // TODO
    }

    @Override
    public void invalidateAll() {
        for (CachingTier<K, V> cachingTier : cachingTierList) {
            cachingTier.invalidateAll();
        }
    }

    @Override
    public long count() {
        long totalCount = 0;
        for (CachingTier<K, V> cachingTier : cachingTierList) {
            totalCount += cachingTier.count();
        }
        return totalCount;
    }

    @Override
    public void onRemoval(RemovalNotification<K, V> notification) {
        if (RemovalReason.EVICTED.equals(notification.getRemovalReason())) {
            switch (notification.getTierType()) {
                case ON_HEAP:
                    diskCachingTier.put(notification.getKey(), notification.getValue());
                    break;
                default:
                    break;
            }
        }
        tieredCacheEventListener.onRemoval(notification);
    }

    @Override
    public CachingTier<K, V> getOnHeapCachingTier() {
        return this.onHeapCachingTier;
    }

    private void setRemovalListeners() {
        for (CachingTier<K, V> cachingTier : cachingTierList) {
            cachingTier.setRemovalListener(this);
        }
    }

    private Function<K, CacheValue<V>> getValueFromTierCache() {
        return key -> {
            for (CachingTier<K, V> cachingTier : cachingTierList) {
                V value = cachingTier.get(key);
                if (value != null) {
                    tieredCacheEventListener.onHit(key, value, cachingTier.getTierType());
                    return new CacheValue<>(value, cachingTier.getTierType());
                }
                tieredCacheEventListener.onMiss(key, cachingTier.getTierType());
            }
            return null;
        };
    }

    public static class CacheValue<V> {
        V value;
        TierType source;

        CacheValue(V value, TierType source) {
            this.value = value;
            this.source = source;
        }
    }

    public static class Builder<K, V> {
        private OnHeapCachingTier<K, V> onHeapCachingTier;
        private CachingTier<K, V> diskCachingTier;
        private TieredCacheEventListener<K, V> tieredCacheEventListener;

        public Builder() {}

        public Builder<K, V> setOnHeapCachingTier(OnHeapCachingTier<K, V> onHeapCachingTier) {
            this.onHeapCachingTier = onHeapCachingTier;
            return this;
        }

        public Builder<K, V> setOnDiskCachingTier(CachingTier<K, V> diskCachingTier) {
            this.diskCachingTier = diskCachingTier;
            return this;
        }

        public Builder<K, V> setTieredCacheEventListener(TieredCacheEventListener<K, V> tieredCacheEventListener) {
            this.tieredCacheEventListener = tieredCacheEventListener;
            return this;
        }

        public TieredCacheSpilloverStrategyHandler<K, V> build() {
            return new TieredCacheSpilloverStrategyHandler<K, V>(
                this.onHeapCachingTier,
                this.diskCachingTier,
                this.tieredCacheEventListener
            );
        }
    }

}
