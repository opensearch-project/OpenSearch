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
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.MultiDimensionCacheStats;
import org.opensearch.common.cache.stats.StatsHolder;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

/**
 * This variant of on-heap cache uses OpenSearch custom cache implementation.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.experimental
 */
public class OpenSearchOnHeapCache<K, V> implements ICache<K, V>, RemovalListener<ICacheKey<K>, V> {

    private final Cache<ICacheKey<K>, V> cache;
    private final StatsHolder statsHolder;
    private final RemovalListener<ICacheKey<K>, V> removalListener;
    private final List<String> dimensionNames;
    public static final String TIER_DIMENSION_VALUE = "on_heap";

    public OpenSearchOnHeapCache(Builder<K, V> builder) {
        CacheBuilder<ICacheKey<K>, V> cacheBuilder = CacheBuilder.<ICacheKey<K>, V>builder()
            .setMaximumWeight(builder.getMaxWeightInBytes())
            .weigher(builder.getWeigher())
            .removalListener(this);
        if (builder.getExpireAfterAcess() != null) {
            cacheBuilder.setExpireAfterAccess(builder.getExpireAfterAcess());
        }
        cache = cacheBuilder.build();
        this.dimensionNames = Objects.requireNonNull(builder.dimensionNames, "Dimension names can't be null");
        this.statsHolder = new StatsHolder(dimensionNames, builder.getSettings());
        this.removalListener = builder.getRemovalListener();
    }

    @Override
    public V get(ICacheKey<K> key) {
        V value = cache.get(key);
        if (value != null) {
            statsHolder.incrementHitsByDimensions(key.dimensions);
        } else {
            statsHolder.incrementMissesByDimensions(key.dimensions);
        }
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        cache.put(key, value);
        statsHolder.incrementEntriesByDimensions(key.dimensions);
        statsHolder.incrementMemorySizeByDimensions(key.dimensions, cache.getWeigher().applyAsLong(key, value));
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        V value = cache.computeIfAbsent(key, key1 -> loader.load(key));
        if (!loader.isLoaded()) {
            statsHolder.incrementHitsByDimensions(key.dimensions);
        } else {
            statsHolder.incrementMissesByDimensions(key.dimensions);
            statsHolder.incrementEntriesByDimensions(key.dimensions);
            statsHolder.incrementMemorySizeByDimensions(key.dimensions, cache.getWeigher().applyAsLong(key, value));
        }
        return value;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
        statsHolder.reset();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
        return cache.keys();
    }

    @Override
    public long count() {
        return statsHolder.count();
    }

    @Override
    public void refresh() {
        cache.refresh();
    }

    @Override
    public void close() {}

    @Override
    public CacheStats stats() {
        return new MultiDimensionCacheStats(statsHolder, TIER_DIMENSION_VALUE);
    }

    @Override
    public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
        removalListener.onRemoval(notification);
        statsHolder.decrementEntriesByDimensions(notification.getKey().dimensions);
        statsHolder.incrementMemorySizeByDimensions(
            notification.getKey().dimensions,
            -cache.getWeigher().applyAsLong(notification.getKey(), notification.getValue())
        );

        if (RemovalReason.EVICTED.equals(notification.getRemovalReason())
            || RemovalReason.CAPACITY.equals(notification.getRemovalReason())) {
            statsHolder.incrementEvictionsByDimensions(notification.getKey().dimensions);
        }
    }

    /**
     * Factory to create OpenSearchOnheap cache.
     */
    public static class OpenSearchOnHeapCacheFactory implements Factory {

        public static final String NAME = "opensearch_onheap";

        @Override
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            Map<String, Setting<?>> settingList = OpenSearchOnHeapCacheSettings.getSettingListForCacheType(cacheType);
            Settings settings = config.getSettings();
            return new Builder<K, V>().setDimensionNames(config.getDimensionNames())
                .setMaximumWeightInBytes(((ByteSizeValue) settingList.get(MAXIMUM_SIZE_IN_BYTES_KEY).get(settings)).getBytes())
                .setWeigher(config.getWeigher())
                .setRemovalListener(config.getRemovalListener())
                .setSettings(settings)
                .build();
        }

        @Override
        public String getCacheName() {
            return NAME;
        }
    }

    /**
     * Builder object
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> extends ICacheBuilder<K, V> {
        private List<String> dimensionNames;

        public Builder<K, V> setDimensionNames(List<String> dimensionNames) {
            this.dimensionNames = dimensionNames;
            return this;
        }

        @Override
        public ICache<K, V> build() {
            return new OpenSearchOnHeapCache<K, V>(this);
        }
    }
}
