/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.cache.common.policy.TookTimePolicy;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.policy.CachedQueryResult;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongBiFunction;

import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.DISK_CACHE_ENABLED_SETTING_MAP;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_DISK;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_ON_HEAP;

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
@ExperimentalApi
public class TieredSpilloverCache<K, V> implements ICache<K, V> {

    // Used to avoid caching stale entries in lower tiers.
    private static final List<RemovalReason> SPILLOVER_REMOVAL_REASONS = List.of(RemovalReason.EVICTED, RemovalReason.CAPACITY);

    private final ICache<K, V> diskCache;
    private final ICache<K, V> onHeapCache;

    // Removal listeners for the individual tiers
    private final RemovalListener<ICacheKey<K>, V> onDiskRemovalListener;
    private final RemovalListener<ICacheKey<K>, V> onHeapRemovalListener;

    // Removal listener from the spillover cache as a whole
    private final RemovalListener<ICacheKey<K>, V> removalListener;

    // In future we want to just read the stats from the individual tiers' statsHolder objects, but this isn't
    // possible right now because of the way computeIfAbsent is implemented.
    private final TieredSpilloverCacheStatsHolder statsHolder;
    private ToLongBiFunction<ICacheKey<K>, V> weigher;
    private final List<String> dimensionNames;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    ReleasableLock readLock = new ReleasableLock(readWriteLock.readLock());
    ReleasableLock writeLock = new ReleasableLock(readWriteLock.writeLock());
    /**
     * Maintains caching tiers in ascending order of cache latency.
     */
    private final Map<ICache<K, V>, Boolean> caches;
    private final Map<ICache<K, V>, String> tierValueMap;
    private final List<Predicate<V>> policies;

    TieredSpilloverCache(Builder<K, V> builder) {
        Objects.requireNonNull(builder.onHeapCacheFactory, "onHeap cache builder can't be null");
        Objects.requireNonNull(builder.diskCacheFactory, "disk cache builder can't be null");
        Objects.requireNonNull(builder.cacheConfig, "cache config can't be null");
        Objects.requireNonNull(builder.cacheConfig.getClusterSettings(), "cluster settings can't be null");
        this.removalListener = Objects.requireNonNull(builder.removalListener, "Removal listener can't be null");

        this.onHeapRemovalListener = new HeapTierRemovalListener(this);
        this.onDiskRemovalListener = new DiskTierRemovalListener(this);
        this.weigher = Objects.requireNonNull(builder.cacheConfig.getWeigher(), "Weigher can't be null");

        this.onHeapCache = builder.onHeapCacheFactory.create(
            new CacheConfig.Builder<K, V>().setRemovalListener(onHeapRemovalListener)
                .setKeyType(builder.cacheConfig.getKeyType())
                .setValueType(builder.cacheConfig.getValueType())
                .setSettings(builder.cacheConfig.getSettings())
                .setWeigher(builder.cacheConfig.getWeigher())
                .setDimensionNames(builder.cacheConfig.getDimensionNames())
                .setMaxSizeInBytes(builder.cacheConfig.getMaxSizeInBytes())
                .setExpireAfterAccess(builder.cacheConfig.getExpireAfterAccess())
                .setClusterSettings(builder.cacheConfig.getClusterSettings())
                .setUseNoopStats(true)
                .build(),
            builder.cacheType,
            builder.cacheFactories

        );
        this.diskCache = builder.diskCacheFactory.create(
            new CacheConfig.Builder<K, V>().setRemovalListener(onDiskRemovalListener)
                .setKeyType(builder.cacheConfig.getKeyType())
                .setValueType(builder.cacheConfig.getValueType())
                .setSettings(builder.cacheConfig.getSettings())
                .setWeigher(builder.cacheConfig.getWeigher())
                .setDimensionNames(builder.cacheConfig.getDimensionNames())
                .setUseNoopStats(true)
                .build(),
            builder.cacheType,
            builder.cacheFactories
        );
        Boolean isDiskCacheEnabled = DISK_CACHE_ENABLED_SETTING_MAP.get(builder.cacheType).get(builder.cacheConfig.getSettings());
        LinkedHashMap<ICache<K, V>, Boolean> cacheListMap = new LinkedHashMap<>();
        cacheListMap.put(onHeapCache, true);
        cacheListMap.put(diskCache, isDiskCacheEnabled);
        this.caches = Collections.synchronizedMap(cacheListMap);

        this.dimensionNames = builder.cacheConfig.getDimensionNames();
        this.tierValueMap = Map.of(onHeapCache, TIER_DIMENSION_VALUE_ON_HEAP, diskCache, TIER_DIMENSION_VALUE_DISK);
        // Pass "tier" as the innermost dimension name, in addition to whatever dimensions are specified for the cache as a whole
        this.statsHolder = new TieredSpilloverCacheStatsHolder(dimensionNames);
        this.policies = builder.policies; // Will never be null; builder initializes it to an empty list
        builder.cacheConfig.getClusterSettings()
            .addSettingsUpdateConsumer(DISK_CACHE_ENABLED_SETTING_MAP.get(builder.cacheType), this::enableDisableDiskCache);
    }

    // Package private for testing
    ICache<K, V> getOnHeapCache() {
        return onHeapCache;
    }

    // Package private for testing
    ICache<K, V> getDiskCache() {
        return diskCache;
    }

    // Package private for testing.
    void enableDisableDiskCache(Boolean isDiskCacheEnabled) {
        // When disk cache is disabled, we are not clearing up the disk cache entries yet as that should be part of
        // separate cache/clear API.
        this.caches.put(diskCache, isDiskCacheEnabled);
    }

    @Override
    public V get(ICacheKey<K> key) {
        return getValueFromTieredCache().apply(key);
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        try (ReleasableLock ignore = writeLock.acquire()) {
            onHeapCache.put(key, value);
            updateStatsOnPut(TIER_DIMENSION_VALUE_ON_HEAP, key, value);
        }
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        V cacheValue = getValueFromTieredCache().apply(key);
        if (cacheValue == null) {
            // Add the value to the onHeap cache. We are calling computeIfAbsent which does another get inside.
            // This is needed as there can be many requests for the same key at the same time and we only want to load
            // the value once.
            V value = null;
            try (ReleasableLock ignore = writeLock.acquire()) {
                value = onHeapCache.computeIfAbsent(key, loader);
                if (loader.isLoaded()) {
                    // The value was just computed and added to the cache
                    updateStatsOnPut(TIER_DIMENSION_VALUE_ON_HEAP, key, value);
                }
            }
            return value;
        }
        return cacheValue;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        // We are trying to invalidate the key from all caches though it would be present in only of them.
        // Doing this as we don't know where it is located. We could do a get from both and check that, but what will
        // also trigger a hit/miss listener event, so ignoring it for now.
        // We don't update stats here, as this is handled by the removal listeners for the tiers.
        try (ReleasableLock ignore = writeLock.acquire()) {
            // for (Tuple<ICache<K, V>, String> pair : cacheAndTierValueList) {
            for (Map.Entry<ICache<K, V>, String> cacheEntry : tierValueMap.entrySet()) {
                if (key.getDropStatsForDimensions()) {
                    List<String> dimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, cacheEntry.getValue());
                    statsHolder.removeDimensions(dimensionValues);
                }
                if (key.key != null) {
                    cacheEntry.getKey().invalidate(key);
                }
            }
        }
    }

    @Override
    public void invalidateAll() {
        try (ReleasableLock ignore = writeLock.acquire()) {
            for (Map.Entry<ICache<K, V>, Boolean> cacheEntry : caches.entrySet()) {
                cacheEntry.getKey().invalidateAll();
            }
        }
        statsHolder.reset();
    }

    /**
     * Provides an iteration over both onHeap and disk keys. This is not protected from any mutations to the cache.
     * @return An iterable over (onHeap + disk) keys
     */
    @SuppressWarnings({ "unchecked" })
    @Override
    public Iterable<ICacheKey<K>> keys() {
        List<Iterable<ICacheKey<K>>> iterableList = new ArrayList<>();
        for (Map.Entry<ICache<K, V>, Boolean> cacheEntry : caches.entrySet()) {
            iterableList.add(cacheEntry.getKey().keys());
        }
        Iterable<ICacheKey<K>>[] iterables = (Iterable<ICacheKey<K>>[]) iterableList.toArray(new Iterable<?>[0]);
        return new ConcatenatedIterables<>(iterables);
    }

    @Override
    public long count() {
        // Count for all the tiers irrespective of whether they are enabled or not. As eventually
        // this will turn to zero once cache is cleared up either via invalidation or manually.
        return statsHolder.count();
    }

    @Override
    public void refresh() {
        try (ReleasableLock ignore = writeLock.acquire()) {
            for (Map.Entry<ICache<K, V>, Boolean> cacheEntry : caches.entrySet()) {
                cacheEntry.getKey().refresh();
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<ICache<K, V>, Boolean> cacheEntry : caches.entrySet()) {
            // Close all the caches here irrespective of whether they are enabled or not.
            cacheEntry.getKey().close();
        }
    }

    @Override
    public ImmutableCacheStatsHolder stats() {
        return statsHolder.getImmutableCacheStatsHolder();
    }

    private Function<ICacheKey<K>, V> getValueFromTieredCache() {
        return key -> {
            try (ReleasableLock ignore = readLock.acquire()) {
                for (Map.Entry<ICache<K, V>, Boolean> cacheEntry : caches.entrySet()) {
                    if (cacheEntry.getValue()) {
                        V value = cacheEntry.getKey().get(key);
                        // Get the tier value corresponding to this cache
                        String tierValue = tierValueMap.get(cacheEntry.getKey());
                        List<String> dimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, tierValue);
                        if (value != null) {
                            statsHolder.incrementHits(dimensionValues);
                            return value;
                        } else {
                            statsHolder.incrementMisses(dimensionValues);
                        }
                    }
                }
                return null;
            }
        };
    }

    void handleRemovalFromHeapTier(RemovalNotification<ICacheKey<K>, V> notification) {
        ICacheKey<K> key = notification.getKey();
        boolean wasEvicted = false;
        if (caches.get(diskCache)
            && SPILLOVER_REMOVAL_REASONS.contains(notification.getRemovalReason())
            && evaluatePolicies(notification.getValue())) {
            try (ReleasableLock ignore = writeLock.acquire()) {
                diskCache.put(key, notification.getValue()); // spill over to the disk tier and increment its stats
                updateStatsOnPut(TIER_DIMENSION_VALUE_DISK, key, notification.getValue());
            }
            wasEvicted = true;
        } else {
            // If the removal was for another reason, send this notification to the TSC's removal listener, as the value is leaving the TSC
            // entirely
            removalListener.onRemoval(notification);
        }
        updateStatsOnRemoval(TIER_DIMENSION_VALUE_ON_HEAP, wasEvicted, key, notification.getValue());
    }

    void handleRemovalFromDiskTier(RemovalNotification<ICacheKey<K>, V> notification) {
        // Values removed from the disk tier leave the TSC entirely
        removalListener.onRemoval(notification);
        boolean wasEvicted = SPILLOVER_REMOVAL_REASONS.contains(notification.getRemovalReason());
        updateStatsOnRemoval(TIER_DIMENSION_VALUE_DISK, wasEvicted, notification.getKey(), notification.getValue());
    }

    void updateStatsOnRemoval(String removedFromTierValue, boolean wasEvicted, ICacheKey<K> key, V value) {
        List<String> dimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, removedFromTierValue);
        if (wasEvicted) {
            statsHolder.incrementEvictions(dimensionValues);
        }
        statsHolder.decrementEntries(dimensionValues);
        statsHolder.decrementSizeInBytes(dimensionValues, weigher.applyAsLong(key, value));
    }

    void updateStatsOnPut(String destinationTierValue, ICacheKey<K> key, V value) {
        List<String> dimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, destinationTierValue);
        statsHolder.incrementEntries(dimensionValues);
        statsHolder.incrementSizeInBytes(dimensionValues, weigher.applyAsLong(key, value));
    }

    boolean evaluatePolicies(V value) {
        for (Predicate<V> policy : policies) {
            if (!policy.test(value)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A class which receives removal events from the heap tier.
     */
    private class HeapTierRemovalListener implements RemovalListener<ICacheKey<K>, V> {
        private final TieredSpilloverCache<K, V> tsc;

        HeapTierRemovalListener(TieredSpilloverCache<K, V> tsc) {
            this.tsc = tsc;
        }

        @Override
        public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
            tsc.handleRemovalFromHeapTier(notification);
        }
    }

    /**
     * A class which receives removal events from the disk tier.
     */
    private class DiskTierRemovalListener implements RemovalListener<ICacheKey<K>, V> {
        private final TieredSpilloverCache<K, V> tsc;

        DiskTierRemovalListener(TieredSpilloverCache<K, V> tsc) {
            this.tsc = tsc;
        }

        @Override
        public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
            tsc.handleRemovalFromDiskTier(notification);
        }
    }

    /**
     * ConcatenatedIterables which combines cache iterables and supports remove() functionality as well if underlying
     * iterator supports it.
     * @param <K> Type of key.
     */
    static class ConcatenatedIterables<K> implements Iterable<K> {

        final Iterable<K>[] iterables;

        ConcatenatedIterables(Iterable<K>[] iterables) {
            this.iterables = iterables;
        }

        @SuppressWarnings({ "unchecked" })
        @Override
        public Iterator<K> iterator() {
            Iterator<K>[] iterators = (Iterator<K>[]) new Iterator<?>[iterables.length];
            for (int i = 0; i < iterables.length; i++) {
                iterators[i] = iterables[i].iterator();
            }
            return new ConcatenatedIterator<>(iterators);
        }

        static class ConcatenatedIterator<T> implements Iterator<T> {
            private final Iterator<T>[] iterators;
            private int currentIteratorIndex;
            private Iterator<T> currentIterator;

            public ConcatenatedIterator(Iterator<T>[] iterators) {
                this.iterators = iterators;
                this.currentIteratorIndex = 0;
                this.currentIterator = iterators[currentIteratorIndex];
            }

            @Override
            public boolean hasNext() {
                while (!currentIterator.hasNext()) {
                    currentIteratorIndex++;
                    if (currentIteratorIndex == iterators.length) {
                        return false;
                    }
                    currentIterator = iterators[currentIteratorIndex];
                }
                return true;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return currentIterator.next();
            }

            @Override
            public void remove() {
                currentIterator.remove();
            }
        }
    }

    /**
     * Factory to create TieredSpilloverCache objects.
     */
    public static class TieredSpilloverCacheFactory implements ICache.Factory {

        /**
         * Defines cache name
         */
        public static final String TIERED_SPILLOVER_CACHE_NAME = "tiered_spillover";

        /**
         * Default constructor
         */
        public TieredSpilloverCacheFactory() {}

        @Override
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            Settings settings = config.getSettings();
            Setting<String> onHeapSetting = TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(
                cacheType.getSettingPrefix()
            );
            String onHeapCacheStoreName = onHeapSetting.get(settings);
            if (!cacheFactories.containsKey(onHeapCacheStoreName)) {
                throw new IllegalArgumentException(
                    "No associated onHeapCache found for tieredSpilloverCache for " + "cacheType:" + cacheType
                );
            }
            ICache.Factory onHeapCacheFactory = cacheFactories.get(onHeapCacheStoreName);

            Setting<String> onDiskSetting = TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORE_NAME.getConcreteSettingForNamespace(
                cacheType.getSettingPrefix()
            );
            String diskCacheStoreName = onDiskSetting.get(settings);
            if (!cacheFactories.containsKey(diskCacheStoreName)) {
                throw new IllegalArgumentException(
                    "No associated diskCache found for tieredSpilloverCache for " + "cacheType:" + cacheType
                );
            }
            ICache.Factory diskCacheFactory = cacheFactories.get(diskCacheStoreName);

            TimeValue diskPolicyThreshold = TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(cacheType)
                .get(settings);
            Function<V, CachedQueryResult.PolicyValues> cachedResultParser = Objects.requireNonNull(
                config.getCachedResultParser(),
                "Cached result parser fn can't be null"
            );

            return new Builder<K, V>().setDiskCacheFactory(diskCacheFactory)
                .setOnHeapCacheFactory(onHeapCacheFactory)
                .setRemovalListener(config.getRemovalListener())
                .setCacheConfig(config)
                .setCacheType(cacheType)
                .addPolicy(new TookTimePolicy<V>(diskPolicyThreshold, cachedResultParser, config.getClusterSettings(), cacheType))
                .build();
        }

        @Override
        public String getCacheName() {
            return TIERED_SPILLOVER_CACHE_NAME;
        }
    }

    /**
     * Builder object for tiered spillover cache.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> {
        private ICache.Factory onHeapCacheFactory;
        private ICache.Factory diskCacheFactory;
        private RemovalListener<ICacheKey<K>, V> removalListener;
        private CacheConfig<K, V> cacheConfig;
        private CacheType cacheType;
        private Map<String, ICache.Factory> cacheFactories;
        private final ArrayList<Predicate<V>> policies = new ArrayList<>();

        /**
         * Default constructor
         */
        public Builder() {}

        /**
         * Set onHeap cache factory
         * @param onHeapCacheFactory Factory for onHeap cache.
         * @return builder
         */
        public Builder<K, V> setOnHeapCacheFactory(ICache.Factory onHeapCacheFactory) {
            this.onHeapCacheFactory = onHeapCacheFactory;
            return this;
        }

        /**
         * Set disk cache factory
         * @param diskCacheFactory Factory for disk cache.
         * @return builder
         */
        public Builder<K, V> setDiskCacheFactory(ICache.Factory diskCacheFactory) {
            this.diskCacheFactory = diskCacheFactory;
            return this;
        }

        /**
         * Set removal listener for tiered cache.
         * @param removalListener Removal listener
         * @return builder
         */
        public Builder<K, V> setRemovalListener(RemovalListener<ICacheKey<K>, V> removalListener) {
            this.removalListener = removalListener;
            return this;
        }

        /**
         * Set cache config.
         * @param cacheConfig cache config.
         * @return builder
         */
        public Builder<K, V> setCacheConfig(CacheConfig<K, V> cacheConfig) {
            this.cacheConfig = cacheConfig;
            return this;
        }

        /**
         * Set cache type.
         * @param cacheType Cache type
         * @return builder
         */
        public Builder<K, V> setCacheType(CacheType cacheType) {
            this.cacheType = cacheType;
            return this;
        }

        /**
         * Set cache factories
         * @param cacheFactories cache factories
         * @return builder
         */
        public Builder<K, V> setCacheFactories(Map<String, ICache.Factory> cacheFactories) {
            this.cacheFactories = cacheFactories;
            return this;
        }

        /**
         * Set a cache policy to be used to limit access to this cache's disk tier.
         * @param policy the policy
         * @return builder
         */
        public Builder<K, V> addPolicy(Predicate<V> policy) {
            this.policies.add(policy);
            return this;
        }

        /**
         * Set multiple policies to be used to limit access to this cache's disk tier.
         * @param policies the policies
         * @return builder
         */
        public Builder<K, V> addPolicies(List<Predicate<V>> policies) {
            this.policies.addAll(policies);
            return this;
        }

        /**
         * Build tiered spillover cache.
         * @return TieredSpilloverCache
         */
        public TieredSpilloverCache<K, V> build() {
            return new TieredSpilloverCache<>(this);
        }
    }
}
