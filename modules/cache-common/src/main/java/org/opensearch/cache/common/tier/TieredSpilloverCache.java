/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.opensearch.common.collect.Tuple;
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
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongBiFunction;

import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.DISK_CACHE_ENABLED_SETTING_MAP;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORAGE_PATH;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORE_SIZE;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_SIZE;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.TIERED_SPILLOVER_SEGMENTS;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_DISK;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_ON_HEAP;
import static org.opensearch.common.cache.settings.CacheSettings.INVALID_SEGMENT_COUNT_EXCEPTION_MESSAGE;
import static org.opensearch.common.cache.settings.CacheSettings.VALID_SEGMENT_COUNT_VALUES;

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
    private static final Logger logger = LogManager.getLogger(TieredSpilloverCache.class);

    static final String ZERO_SEGMENT_COUNT_EXCEPTION_MESSAGE = "Segment count cannot be less than one for tiered cache";
    static final String INVALID_STORAGE_PATH_EXCEPTION_MESSAGE = "Storage path for disk cache cannot be null or empty";

    // In future we want to just read the stats from the individual tiers' statsHolder objects, but this isn't
    // possible right now because of the way computeIfAbsent is implemented.
    private final TieredSpilloverCacheStatsHolder statsHolder;
    private final List<String> dimensionNames;

    private final int numberOfSegments;

    final TieredSpilloverCacheSegment<K, V>[] tieredSpilloverCacheSegments;
    final String diskCacheStoragePath;

    /**
     * This map is used to handle concurrent requests for same key in computeIfAbsent() to ensure we load the value
     * only once.
     */
    Map<ICacheKey<K>, CompletableFuture<Tuple<ICacheKey<K>, V>>> completableFutureMap = new ConcurrentHashMap<>();

    @SuppressWarnings({ "unchecked", "rawtypes" })
    TieredSpilloverCache(Builder<K, V> builder) {
        Objects.requireNonNull(builder.cacheConfig, "cache config can't be null");
        Objects.requireNonNull(builder.cacheConfig.getSettings(), "settings can't be null");
        if (builder.numberOfSegments <= 0) {
            throw new IllegalArgumentException(ZERO_SEGMENT_COUNT_EXCEPTION_MESSAGE);
        }
        if (builder.diskCacheStoragePath == null || builder.diskCacheStoragePath.isBlank()) {
            throw new IllegalArgumentException(INVALID_STORAGE_PATH_EXCEPTION_MESSAGE);
        }
        this.diskCacheStoragePath = builder.diskCacheStoragePath;
        this.numberOfSegments = builder.numberOfSegments;
        Boolean isDiskCacheEnabled = DISK_CACHE_ENABLED_SETTING_MAP.get(builder.cacheType).get(builder.cacheConfig.getSettings());
        this.dimensionNames = builder.cacheConfig.getDimensionNames();
        // Pass "tier" as the innermost dimension name, in addition to whatever dimensions are specified for the cache as a whole
        this.statsHolder = new TieredSpilloverCacheStatsHolder(dimensionNames, isDiskCacheEnabled);
        long onHeapCachePerSegmentSizeInBytes = builder.onHeapCacheSizeInBytes / this.numberOfSegments;
        long diskCachePerSegmentSizeInBytes = builder.diskCacheSizeInBytes / this.numberOfSegments;
        if (onHeapCachePerSegmentSizeInBytes <= 0) {
            throw new IllegalArgumentException("Per segment size for onHeap cache within Tiered cache should be " + "greater than 0");
        }
        if (diskCachePerSegmentSizeInBytes <= 0) {
            throw new IllegalArgumentException("Per segment size for disk cache within Tiered cache should be " + "greater than 0");
        }
        this.tieredSpilloverCacheSegments = new TieredSpilloverCacheSegment[this.numberOfSegments];
        for (int i = 0; i < numberOfSegments; i++) {
            tieredSpilloverCacheSegments[i] = new TieredSpilloverCacheSegment<K, V>(
                builder,
                statsHolder,
                i + 1,
                this.numberOfSegments,
                onHeapCachePerSegmentSizeInBytes,
                diskCachePerSegmentSizeInBytes
            );
        }
        builder.cacheConfig.getClusterSettings()
            .addSettingsUpdateConsumer(DISK_CACHE_ENABLED_SETTING_MAP.get(builder.cacheType), this::enableDisableDiskCache);
    }

    static class TieredSpilloverCacheSegment<K, V> implements ICache<K, V> {

        private final ICache<K, V> diskCache;
        private final ICache<K, V> onHeapCache;

        // Removal listeners for the individual tiers
        private final RemovalListener<ICacheKey<K>, V> onDiskRemovalListener;
        private final RemovalListener<ICacheKey<K>, V> onHeapRemovalListener;

        // Removal listener from the spillover cache as a whole
        private final RemovalListener<ICacheKey<K>, V> removalListener;

        private ToLongBiFunction<ICacheKey<K>, V> weigher;
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        ReleasableLock readLock = new ReleasableLock(readWriteLock.readLock());
        ReleasableLock writeLock = new ReleasableLock(readWriteLock.writeLock());

        private final Map<ICache<K, V>, TierInfo> caches;

        private final List<Predicate<V>> policies;

        private final TieredSpilloverCacheStatsHolder statsHolder;

        /**
         * This map is used to handle concurrent requests for same key in computeIfAbsent() to ensure we load the value
         * only once.
         */
        Map<ICacheKey<K>, CompletableFuture<Tuple<ICacheKey<K>, V>>> completableFutureMap = new ConcurrentHashMap<>();

        TieredSpilloverCacheSegment(
            Builder<K, V> builder,
            TieredSpilloverCacheStatsHolder statsHolder,
            int segmentNumber,
            int numberOfSegments,
            long onHeapCacheSizeInBytes,
            long diskCacheSizeInBytes
        ) {
            Objects.requireNonNull(builder.onHeapCacheFactory, "onHeap cache builder can't be null");
            Objects.requireNonNull(builder.diskCacheFactory, "disk cache builder can't be null");
            Objects.requireNonNull(builder.cacheConfig, "cache config can't be null");
            Objects.requireNonNull(builder.cacheConfig.getClusterSettings(), "cluster settings can't be null");
            this.removalListener = Objects.requireNonNull(builder.removalListener, "Removal listener can't be null");
            this.statsHolder = statsHolder;

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
                    .setMaxSizeInBytes(onHeapCacheSizeInBytes)
                    .setExpireAfterAccess(builder.cacheConfig.getExpireAfterAccess())
                    .setClusterSettings(builder.cacheConfig.getClusterSettings())
                    .setSegmentCount(1) // We don't need to make underlying caches multi-segmented
                    .setStatsTrackingEnabled(false)
                    .setCacheAlias("tiered_on_heap#" + segmentNumber)
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
                    .setKeySerializer(builder.cacheConfig.getKeySerializer())
                    .setValueSerializer(builder.cacheConfig.getValueSerializer())
                    .setDimensionNames(builder.cacheConfig.getDimensionNames())
                    .setSegmentCount(1) // We don't need to make underlying caches multi-segmented
                    .setStatsTrackingEnabled(false)
                    .setMaxSizeInBytes(diskCacheSizeInBytes)
                    .setStoragePath(builder.diskCacheStoragePath + "/" + segmentNumber)
                    .setCacheAlias("tiered_disk_cache#" + segmentNumber)
                    .build(),
                builder.cacheType,
                builder.cacheFactories
            );

            Boolean isDiskCacheEnabled = DISK_CACHE_ENABLED_SETTING_MAP.get(builder.cacheType).get(builder.cacheConfig.getSettings());
            LinkedHashMap<ICache<K, V>, TierInfo> cacheListMap = new LinkedHashMap<>();
            cacheListMap.put(onHeapCache, new TierInfo(true, TIER_DIMENSION_VALUE_ON_HEAP));
            cacheListMap.put(diskCache, new TierInfo(isDiskCacheEnabled, TIER_DIMENSION_VALUE_DISK));
            this.caches = Collections.synchronizedMap(cacheListMap);
            this.policies = builder.policies; // Will never be null; builder initializes it to an empty list
        }

        // Package private for testing
        ICache<K, V> getOnHeapCache() {
            return onHeapCache;
        }

        // Package private for testing
        ICache<K, V> getDiskCache() {
            return diskCache;
        }

        void enableDisableDiskCache(Boolean isDiskCacheEnabled) {
            // When disk cache is disabled, we are not clearing up the disk cache entries yet as that should be part of
            // separate cache/clear API.
            this.caches.put(diskCache, new TierInfo(isDiskCacheEnabled, TIER_DIMENSION_VALUE_DISK));
            this.statsHolder.setDiskCacheEnabled(isDiskCacheEnabled);
        }

        @Override
        public V get(ICacheKey<K> key) {
            Tuple<V, String> cacheValueTuple = getValueFromTieredCache(true).apply(key);
            if (cacheValueTuple == null) {
                return null;
            }
            return cacheValueTuple.v1();
        }

        @Override
        public void put(ICacheKey<K> key, V value) {
            // First check in case the key is already present in either of tiers.
            Tuple<V, String> cacheValueTuple = getValueFromTieredCache(true).apply(key);
            if (cacheValueTuple == null) {
                // In case it is not present in any tier, put it inside onHeap cache by default.
                try (ReleasableLock ignore = writeLock.acquire()) {
                    onHeapCache.put(key, value);
                }
                updateStatsOnPut(TIER_DIMENSION_VALUE_ON_HEAP, key, value);
            } else {
                // Put it inside desired tier.
                try (ReleasableLock ignore = writeLock.acquire()) {
                    for (Map.Entry<ICache<K, V>, TierInfo> entry : this.caches.entrySet()) {
                        if (cacheValueTuple.v2().equals(entry.getValue().tierName)) {
                            entry.getKey().put(key, value);
                        }
                    }
                    updateStatsOnPut(cacheValueTuple.v2(), key, value);
                }
            }
        }

        @Override
        public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
            // Don't capture stats in the initial getValueFromTieredCache(). If we have concurrent requests for the same key,
            // and it only has to be loaded one time, we should report one miss and the rest hits. But, if we do stats in
            // getValueFromTieredCache(),
            // we will see all misses. Instead, handle stats in computeIfAbsent().
            Tuple<V, String> cacheValueTuple;
            CompletableFuture<Tuple<ICacheKey<K>, V>> future = null;
            try (ReleasableLock ignore = readLock.acquire()) {
                cacheValueTuple = getValueFromTieredCache(false).apply(key);
                if (cacheValueTuple == null) {
                    // Only one of the threads will succeed putting a future into map for the same key.
                    // Rest will fetch existing future and wait on that to complete.
                    future = completableFutureMap.putIfAbsent(key, new CompletableFuture<>());
                }
            }
            List<String> heapDimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, TIER_DIMENSION_VALUE_ON_HEAP);
            List<String> diskDimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, TIER_DIMENSION_VALUE_DISK);

            if (cacheValueTuple == null) {
                // Add the value to the onHeap cache. We are calling computeIfAbsent which does another get inside.
                // This is needed as there can be many requests for the same key at the same time and we only want to load
                // the value once.
                V value = compute(key, loader, future);
                // Handle stats
                if (loader.isLoaded()) {
                    // The value was just computed and added to the cache by this thread. Register a miss for the heap cache, and the disk
                    // cache
                    // if present
                    updateStatsOnPut(TIER_DIMENSION_VALUE_ON_HEAP, key, value);
                    statsHolder.incrementMisses(heapDimensionValues);
                    if (caches.get(diskCache).isEnabled()) {
                        statsHolder.incrementMisses(diskDimensionValues);
                    }
                } else {
                    // Another thread requesting this key already loaded the value. Register a hit for the heap cache
                    statsHolder.incrementHits(heapDimensionValues);
                }
                return value;
            } else {
                // Handle stats for an initial hit from getValueFromTieredCache()
                if (cacheValueTuple.v2().equals(TIER_DIMENSION_VALUE_ON_HEAP)) {
                    // A hit for the heap tier
                    statsHolder.incrementHits(heapDimensionValues);
                } else if (cacheValueTuple.v2().equals(TIER_DIMENSION_VALUE_DISK)) {
                    // Miss for the heap tier, hit for the disk tier
                    statsHolder.incrementMisses(heapDimensionValues);
                    statsHolder.incrementHits(diskDimensionValues);
                }
            }
            return cacheValueTuple.v1();
        }

        private V compute(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader, CompletableFuture<Tuple<ICacheKey<K>, V>> future)
            throws Exception {
            // Handler to handle results post-processing. Takes a tuple<key, value> or exception as an input and returns
            // the value. Also before returning value, puts the value in cache.
            BiFunction<Tuple<ICacheKey<K>, V>, Throwable, Void> handler = (pair, ex) -> {
                if (pair != null) {
                    try (ReleasableLock ignore = writeLock.acquire()) {
                        onHeapCache.put(pair.v1(), pair.v2());
                    } catch (Exception e) {
                        // TODO: Catch specific exceptions to know whether this resulted from cache or underlying removal
                        // listeners/stats. Needs better exception handling at underlying layers.For now swallowing
                        // exception.
                        logger.warn("Exception occurred while putting item onto heap cache", e);
                    }
                } else {
                    if (ex != null) {
                        logger.warn("Exception occurred while trying to compute the value", ex);
                    }
                }
                completableFutureMap.remove(key);// Remove key from map as not needed anymore.
                return null;
            };
            V value = null;
            if (future == null) {
                future = completableFutureMap.get(key);
                future.handle(handler);
                try {
                    value = loader.load(key);
                } catch (Exception ex) {
                    future.completeExceptionally(ex);
                    throw new ExecutionException(ex);
                }
                if (value == null) {
                    NullPointerException npe = new NullPointerException("Loader returned a null value");
                    future.completeExceptionally(npe);
                    throw new ExecutionException(npe);
                } else {
                    future.complete(new Tuple<>(key, value));
                }
            } else {
                try {
                    value = future.get().v2();
                } catch (InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return value;
        }

        @Override
        public void invalidate(ICacheKey<K> key) {
            for (Map.Entry<ICache<K, V>, TierInfo> cacheEntry : caches.entrySet()) {
                if (key.getDropStatsForDimensions()) {
                    List<String> dimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, cacheEntry.getValue().tierName);
                    statsHolder.removeDimensions(dimensionValues);
                }
                if (key.key != null) {
                    try (ReleasableLock ignore = writeLock.acquire()) {
                        cacheEntry.getKey().invalidate(key);
                    }
                }
            }
        }

        @Override
        public void invalidateAll() {
            try (ReleasableLock ignore = writeLock.acquire()) {
                for (Map.Entry<ICache<K, V>, TierInfo> cacheEntry : caches.entrySet()) {
                    cacheEntry.getKey().invalidateAll();
                }
            }
            statsHolder.reset();
        }

        @SuppressWarnings({ "unchecked" })
        @Override
        public Iterable<ICacheKey<K>> keys() {
            List<Iterable<ICacheKey<K>>> iterableList = new ArrayList<>();
            for (Map.Entry<ICache<K, V>, TierInfo> cacheEntry : caches.entrySet()) {
                iterableList.add(cacheEntry.getKey().keys());
            }
            Iterable<ICacheKey<K>>[] iterables = (Iterable<ICacheKey<K>>[]) iterableList.toArray(new Iterable<?>[0]);
            return new ConcatenatedIterables<>(iterables);
        }

        @Override
        public long count() {
            return onHeapCache.count() + diskCache.count();
        }

        @Override
        public void refresh() {
            try (ReleasableLock ignore = writeLock.acquire()) {
                for (Map.Entry<ICache<K, V>, TierInfo> cacheEntry : caches.entrySet()) {
                    cacheEntry.getKey().refresh();
                }
            }
        }

        @Override
        public ImmutableCacheStatsHolder stats(String[] levels) {
            return null;
        }

        @Override
        public void close() throws IOException {
            for (Map.Entry<ICache<K, V>, TierInfo> cacheEntry : caches.entrySet()) {
                // Close all the caches here irrespective of whether they are enabled or not.
                cacheEntry.getKey().close();
            }
        }

        void handleRemovalFromHeapTier(RemovalNotification<ICacheKey<K>, V> notification) {
            ICacheKey<K> key = notification.getKey();
            boolean wasEvicted = SPILLOVER_REMOVAL_REASONS.contains(notification.getRemovalReason());
            boolean countEvictionTowardsTotal = false; // Don't count this eviction towards the cache's total if it ends up in the disk tier
            boolean exceptionOccurredOnDiskCachePut = false;
            boolean canCacheOnDisk = caches.get(diskCache).isEnabled() && wasEvicted && evaluatePolicies(notification.getValue());
            if (canCacheOnDisk) {
                try (ReleasableLock ignore = writeLock.acquire()) {
                    diskCache.put(key, notification.getValue()); // spill over to the disk tier and increment its stats
                } catch (Exception ex) {
                    // TODO: Catch specific exceptions. Needs better exception handling. We are just swallowing exception
                    // in this case as it shouldn't cause upstream request to fail.
                    logger.warn("Exception occurred while putting item to disk cache", ex);
                    exceptionOccurredOnDiskCachePut = true;
                }
                if (!exceptionOccurredOnDiskCachePut) {
                    updateStatsOnPut(TIER_DIMENSION_VALUE_DISK, key, notification.getValue());
                }
            }
            if (!canCacheOnDisk || exceptionOccurredOnDiskCachePut) {
                // If the value is not going to the disk cache, send this notification to the TSC's removal listener
                // as the value is leaving the TSC entirely
                removalListener.onRemoval(notification);
                countEvictionTowardsTotal = true;
            }
            updateStatsOnRemoval(TIER_DIMENSION_VALUE_ON_HEAP, wasEvicted, key, notification.getValue(), countEvictionTowardsTotal);
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
         * Get a value from the tiered cache, and the name of the tier it was found in.
         * @param captureStats Whether to record hits/misses for this call of the function
         * @return A tuple of the value and the name of the tier it was found in.
         */
        private Function<ICacheKey<K>, Tuple<V, String>> getValueFromTieredCache(boolean captureStats) {
            return key -> {
                try (ReleasableLock ignore = readLock.acquire()) {
                    for (Map.Entry<ICache<K, V>, TierInfo> cacheEntry : caches.entrySet()) {
                        if (cacheEntry.getValue().isEnabled()) {
                            V value = cacheEntry.getKey().get(key);
                            // Get the tier value corresponding to this cache
                            String tierValue = cacheEntry.getValue().tierName;
                            List<String> dimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, tierValue);
                            if (value != null) {
                                if (captureStats) {
                                    statsHolder.incrementHits(dimensionValues);
                                }
                                return new Tuple<>(value, tierValue);
                            } else if (captureStats) {
                                statsHolder.incrementMisses(dimensionValues);
                            }
                        }
                    }
                    return null;
                }
            };
        }

        void handleRemovalFromDiskTier(RemovalNotification<ICacheKey<K>, V> notification) {
            // Values removed from the disk tier leave the TSC entirely
            removalListener.onRemoval(notification);
            boolean wasEvicted = SPILLOVER_REMOVAL_REASONS.contains(notification.getRemovalReason());
            updateStatsOnRemoval(TIER_DIMENSION_VALUE_DISK, wasEvicted, notification.getKey(), notification.getValue(), true);
        }

        void updateStatsOnRemoval(
            String removedFromTierValue,
            boolean wasEvicted,
            ICacheKey<K> key,
            V value,
            boolean countEvictionTowardsTotal
        ) {
            List<String> dimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, removedFromTierValue);
            if (wasEvicted) {
                statsHolder.incrementEvictions(dimensionValues, countEvictionTowardsTotal);
            }
            statsHolder.decrementItems(dimensionValues);
            statsHolder.decrementSizeInBytes(dimensionValues, weigher.applyAsLong(key, value));
        }

        void updateStatsOnPut(String destinationTierValue, ICacheKey<K> key, V value) {
            List<String> dimensionValues = statsHolder.getDimensionsWithTierValue(key.dimensions, destinationTierValue);
            statsHolder.incrementItems(dimensionValues);
            statsHolder.incrementSizeInBytes(dimensionValues, weigher.applyAsLong(key, value));
        }

        /**
         * A class which receives removal events from the heap tier.
         */
        private class HeapTierRemovalListener implements RemovalListener<ICacheKey<K>, V> {
            private final TieredSpilloverCacheSegment<K, V> tsc;

            HeapTierRemovalListener(TieredSpilloverCacheSegment<K, V> tsc) {
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
            private final TieredSpilloverCacheSegment<K, V> tsc;

            DiskTierRemovalListener(TieredSpilloverCacheSegment<K, V> tsc) {
                this.tsc = tsc;
            }

            @Override
            public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
                tsc.handleRemovalFromDiskTier(notification);
            }
        }
    }

    // Package private for testing.
    String getDiskCacheStoragePath() {
        return this.diskCacheStoragePath;
    }

    // Package private for testing.
    void enableDisableDiskCache(Boolean isDiskCacheEnabled) {
        for (int iter = 0; iter < this.numberOfSegments; iter++) {
            tieredSpilloverCacheSegments[iter].enableDisableDiskCache(isDiskCacheEnabled);
        }
        this.statsHolder.setDiskCacheEnabled(isDiskCacheEnabled);
    }

    // Package private for testing.
    TieredSpilloverCacheSegment<K, V> getTieredCacheSegment(ICacheKey<K> key) {
        return tieredSpilloverCacheSegments[getSegmentNumber(key)];
    }

    int getSegmentNumber(ICacheKey<K> key) {
        return key.hashCode() & (this.numberOfSegments - 1);
    }

    int getNumberOfSegments() {
        return tieredSpilloverCacheSegments.length;
    }

    @Override
    public V get(ICacheKey<K> key) {
        TieredSpilloverCacheSegment<K, V> tieredSpilloverCacheSegment = getTieredCacheSegment(key);
        return tieredSpilloverCacheSegment.get(key);
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        TieredSpilloverCacheSegment<K, V> tieredSpilloverCacheSegment = getTieredCacheSegment(key);
        tieredSpilloverCacheSegment.put(key, value);
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        TieredSpilloverCacheSegment<K, V> tieredSpilloverCacheSegment = getTieredCacheSegment(key);
        return tieredSpilloverCacheSegment.computeIfAbsent(key, loader);
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        TieredSpilloverCacheSegment<K, V> tieredSpilloverCacheSegment = getTieredCacheSegment(key);
        tieredSpilloverCacheSegment.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        for (int iter = 0; iter < this.numberOfSegments; iter++) {
            tieredSpilloverCacheSegments[iter].invalidateAll();
        }
    }

    /**
     * Provides an iteration over both onHeap and disk keys. This is not protected from any mutations to the cache.
     * @return An iterable over (onHeap + disk) keys
     */
    @SuppressWarnings({ "unchecked" })
    @Override
    public Iterable<ICacheKey<K>> keys() {
        List<Iterable<ICacheKey<K>>> iterableList = new ArrayList<>();
        for (int iter = 0; iter < this.numberOfSegments; iter++) {
            iterableList.add(tieredSpilloverCacheSegments[iter].keys());
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
        for (int iter = 0; iter < this.numberOfSegments; iter++) {
            tieredSpilloverCacheSegments[iter].refresh();
        }
    }

    @Override
    public void close() throws IOException {
        for (int iter = 0; iter < this.numberOfSegments; iter++) {
            tieredSpilloverCacheSegments[iter].close();
        }
    }

    @Override
    public ImmutableCacheStatsHolder stats(String[] levels) {
        return statsHolder.getImmutableCacheStatsHolder(levels);
    }

    // Package private for testing.
    @SuppressWarnings({ "unchecked" })
    Iterable<ICacheKey<K>> getOnHeapCacheKeys() {
        List<Iterable<ICacheKey<K>>> iterableList = new ArrayList<>();
        for (int iter = 0; iter < this.numberOfSegments; iter++) {
            iterableList.add(tieredSpilloverCacheSegments[iter].onHeapCache.keys());
        }
        Iterable<ICacheKey<K>>[] iterables = (Iterable<ICacheKey<K>>[]) iterableList.toArray(new Iterable<?>[0]);
        return new ConcatenatedIterables<>(iterables);
    }

    // Package private for testing.
    @SuppressWarnings({ "unchecked" })
    Iterable<ICacheKey<K>> getDiskCacheKeys() {
        List<Iterable<ICacheKey<K>>> iterableList = new ArrayList<>();
        for (int iter = 0; iter < this.numberOfSegments; iter++) {
            iterableList.add(tieredSpilloverCacheSegments[iter].diskCache.keys());
        }
        Iterable<ICacheKey<K>>[] iterables = (Iterable<ICacheKey<K>>[]) iterableList.toArray(new Iterable<?>[0]);
        return new ConcatenatedIterables<>(iterables);
    }

    // Package private for testing.
    long onHeapCacheCount() {
        long onHeapCacheEntries = 0;
        for (int iter = 0; iter < this.numberOfSegments; iter++) {
            onHeapCacheEntries += tieredSpilloverCacheSegments[iter].onHeapCache.count();
        }
        return onHeapCacheEntries;
    }

    // Package private for testing.
    long diskCacheCount() {
        long diskCacheEntries = 0;
        for (int iter = 0; iter < this.numberOfSegments; iter++) {
            diskCacheEntries += tieredSpilloverCacheSegments[iter].diskCache.count();
        }
        return diskCacheEntries;
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

    private static class TierInfo {
        AtomicBoolean isEnabled;
        final String tierName;

        TierInfo(boolean isEnabled, String tierName) {
            this.isEnabled = new AtomicBoolean(isEnabled);
            this.tierName = tierName;
        }

        boolean isEnabled() {
            return isEnabled.get();
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

            int numberOfSegments = TIERED_SPILLOVER_SEGMENTS.getConcreteSettingForNamespace(cacheType.getSettingPrefix()).get(settings);

            String storagePath = TIERED_SPILLOVER_DISK_STORAGE_PATH.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
                .get(settings);
            // If we read the storage path directly from the setting, we have to add the segment number at the end.
            if (storagePath == null || storagePath.isBlank()) {
                // In case storage path is not explicitly set by user, use default path.
                if (config.getStoragePath() == null || config.getStoragePath().isBlank()) {
                    throw new IllegalArgumentException(INVALID_STORAGE_PATH_EXCEPTION_MESSAGE);
                }
                storagePath = config.getStoragePath();
            }

            if (!VALID_SEGMENT_COUNT_VALUES.contains(numberOfSegments)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, INVALID_SEGMENT_COUNT_EXCEPTION_MESSAGE, TIERED_SPILLOVER_CACHE_NAME)
                );
            }

            long onHeapCacheSize = TIERED_SPILLOVER_ONHEAP_STORE_SIZE.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
                .get(settings)
                .getBytes();
            long diskCacheSize = TIERED_SPILLOVER_DISK_STORE_SIZE.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
                .get(settings);

            return new Builder<K, V>().setDiskCacheFactory(diskCacheFactory)
                .setOnHeapCacheFactory(onHeapCacheFactory)
                .setRemovalListener(config.getRemovalListener())
                .setCacheConfig(config)
                .setCacheType(cacheType)
                .setNumberOfSegments(numberOfSegments)
                .addPolicy(new TookTimePolicy<V>(diskPolicyThreshold, cachedResultParser, config.getClusterSettings(), cacheType))
                .setOnHeapCacheSizeInBytes(onHeapCacheSize)
                .setDiskCacheSize(diskCacheSize)
                .setDiskCacheStoragePath(storagePath)
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

        private int numberOfSegments;
        private long onHeapCacheSizeInBytes;
        private long diskCacheSizeInBytes;
        private String diskCacheStoragePath;

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
         * Sets number of segments for tiered cache
         * @param numberOfSegments number of segments
         * @return builder
         */
        public Builder<K, V> setNumberOfSegments(int numberOfSegments) {
            this.numberOfSegments = numberOfSegments;
            return this;
        }

        /**
         * Sets onHeap cache size
         * @param onHeapCacheSizeInBytes size of onHeap cache in bytes
         * @return builder
         */
        public Builder<K, V> setOnHeapCacheSizeInBytes(long onHeapCacheSizeInBytes) {
            this.onHeapCacheSizeInBytes = onHeapCacheSizeInBytes;
            return this;
        }

        /**
         * Sets disk cache size
         * @param diskCacheSizeInBytes size of diskCache in bytes
         * @return buider
         */
        public Builder<K, V> setDiskCacheSize(long diskCacheSizeInBytes) {
            this.diskCacheSizeInBytes = diskCacheSizeInBytes;
            return this;
        }

        /**
         * Sets disk cache storage path.
         * @param diskCacheStoragePath storage path for disk cache
         * @return builder
         */
        public Builder<K, V> setDiskCacheStoragePath(String diskCacheStoragePath) {
            this.diskCacheStoragePath = diskCacheStoragePath;
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
