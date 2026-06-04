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
import org.opensearch.common.cache.service.CacheService;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.stats.CacheStatsHolder;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.stats.NoopCacheStatsHolder;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToLongBiFunction;

import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.EXPIRE_AFTER_ACCESS_KEY;
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
    private final CacheStatsHolder cacheStatsHolder;
    private final RemovalListener<ICacheKey<K>, V> removalListener;
    private final List<String> dimensionNames;
    private final ToLongBiFunction<ICacheKey<K>, V> weigher;
    private final boolean statsTrackingEnabled;
    private final long maximumWeight;

    public OpenSearchOnHeapCache(Builder<K, V> builder) {
        this.maximumWeight = builder.getMaxWeightInBytes();
        CacheBuilder<ICacheKey<K>, V> cacheBuilder = CacheBuilder.<ICacheKey<K>, V>builder()
            .setMaximumWeight(builder.getMaxWeightInBytes())
            .weigher(builder.getWeigher())
            .removalListener(this);
        if (builder.getExpireAfterAcess() != null) {
            cacheBuilder.setExpireAfterAccess(builder.getExpireAfterAcess());
        }
        if (builder.getNumberOfSegments() > 0) {
            cacheBuilder.setNumberOfSegments(builder.getNumberOfSegments());
        }
        cache = cacheBuilder.build();
        this.dimensionNames = Objects.requireNonNull(builder.dimensionNames, "Dimension names can't be null");
        this.statsTrackingEnabled = builder.getStatsTrackingEnabled();
        if (statsTrackingEnabled) {
            this.cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, OpenSearchOnHeapCacheFactory.NAME);
        } else {
            this.cacheStatsHolder = NoopCacheStatsHolder.getInstance();
        }
        this.removalListener = builder.getRemovalListener();
        this.weigher = builder.getWeigher();
    }

    // pkg-private for testing
    long getMaximumWeight() {
        return this.maximumWeight;
    }

    @Override
    public V get(ICacheKey<K> key) {
        V value = cache.get(key);
        if (value != null) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
        }
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        cache.put(key, value);
        cacheStatsHolder.incrementItems(key.dimensions);
        cacheStatsHolder.incrementSizeInBytes(key.dimensions, weigher.applyAsLong(key, value));
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        V value = cache.computeIfAbsent(key, key1 -> loader.load(key));
        if (!loader.isLoaded()) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
            cacheStatsHolder.incrementItems(key.dimensions);
            cacheStatsHolder.incrementSizeInBytes(key.dimensions, cache.getWeigher().applyAsLong(key, value));
        }
        return value;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        if (key.getDropStatsForDimensions()) {
            cacheStatsHolder.removeDimensions(key.dimensions);
        }
        if (key.key != null) {
            cache.invalidate(key);
        }
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
        cacheStatsHolder.reset();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
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
    public void close() {}

    @Override
    public ImmutableCacheStatsHolder stats(String[] levels) {
        return cacheStatsHolder.getImmutableCacheStatsHolder(levels);
    }

    @Override
    public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
        removalListener.onRemoval(notification);
        cacheStatsHolder.decrementItems(notification.getKey().dimensions);
        cacheStatsHolder.decrementSizeInBytes(
            notification.getKey().dimensions,
            cache.getWeigher().applyAsLong(notification.getKey(), notification.getValue())
        );

        if (RemovalReason.EVICTED.equals(notification.getRemovalReason())
            || RemovalReason.CAPACITY.equals(notification.getRemovalReason())) {
            cacheStatsHolder.incrementEvictions(notification.getKey().dimensions);
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
            boolean statsTrackingEnabled = config.getStatsTrackingEnabled();
            ICacheBuilder<K, V> builder = new Builder<K, V>().setDimensionNames(config.getDimensionNames())
                .setStatsTrackingEnabled(statsTrackingEnabled)
                .setExpireAfterAccess(((TimeValue) settingList.get(EXPIRE_AFTER_ACCESS_KEY).get(settings)))
                .setWeigher(config.getWeigher())
                .setRemovalListener(config.getRemovalListener());
            Setting<String> cacheSettingForCacheType = CacheSettings.CACHE_TYPE_STORE_NAME.getConcreteSettingForNamespace(
                cacheType.getSettingPrefix()
            );
            long maxSizeInBytes = ((ByteSizeValue) settingList.get(MAXIMUM_SIZE_IN_BYTES_KEY).get(settings)).getBytes();

            if (config.getMaxSizeInBytes() > 0) {
                /*
                Use the cache config value if present.
                This can be passed down from the TieredSpilloverCache when creating individual segments,
                but is not passed in from the IRC if a store name setting is present.
                 */
                builder.setMaximumWeightInBytes(config.getMaxSizeInBytes());
            } else {
                builder.setMaximumWeightInBytes(maxSizeInBytes);
            }
            if (config.getSegmentCount() > 0) {
                builder.setNumberOfSegments(config.getSegmentCount());
            } else {
                builder.setNumberOfSegments(-1); // By default it will use 256 segments.
            }

            if (!CacheService.storeNamePresent(cacheType, settings)) {
                // For backward compatibility as the user intent is to use older settings.
                builder.setMaximumWeightInBytes(config.getMaxSizeInBytes());
                builder.setExpireAfterAccess(config.getExpireAfterAccess());
                builder.setNumberOfSegments(-1); // By default it will use 256 as we don't want to use this setting
                // when user wants to use older default onHeap cache settings.
            }
            return builder.build();
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
