/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.policy.CachedQueryResult;
import org.opensearch.common.cache.serializer.BytesReferenceSerializer;
import org.opensearch.common.cache.service.CacheService;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ToLongBiFunction;

import static org.opensearch.indices.IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING;

/**
 * The indices request cache allows to cache a shard level request stage responses, helping with improving
 * similar requests that are potentially expensive (because of aggs for example). The cache is fully coherent
 * with the semantics of NRT (the index reader cache key is part of the cache key), and relies on size based
 * eviction to evict old reader associated cache entries as well as scheduler reaper to clean readers that
 * are no longer used or closed shards.
 * <p>
 * Currently, the cache is only enabled for count requests, and can only be opted in on an index
 * level setting that can be dynamically changed and defaults to false.
 * <p>
 * There are still several TODOs left in this class, some easily addressable, some more complex, but the support
 * is functional.
 *
 * @opensearch.internal
 */
public final class IndicesRequestCache implements RemovalListener<IndicesRequestCache.Key, BytesReference>, Closeable {

    private static final Logger logger = LogManager.getLogger(IndicesRequestCache.class);

    /**
     * A setting to enable or disable request caching on an index level. Its dynamic by default
     * since we are checking on the cluster state IndexMetadata always.
     */
    public static final Setting<Boolean> INDEX_CACHE_REQUEST_ENABLED_SETTING = Setting.boolSetting(
        "index.requests.cache.enable",
        true,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDICES_CACHE_QUERY_SIZE = Setting.memorySizeSetting(
        "indices.requests.cache.size",
        "1%",
        Property.NodeScope
    );
    public static final Setting<TimeValue> INDICES_CACHE_QUERY_EXPIRE = Setting.positiveTimeSetting(
        "indices.requests.cache.expire",
        new TimeValue(0),
        Property.NodeScope
    );
    public static final Setting<TimeValue> INDICES_REQUEST_CACHE_CLEAN_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "indices.requests.cache.cleanup.interval",
        INDICES_CACHE_CLEAN_INTERVAL_SETTING,
        Property.NodeScope
    );
    public static final Setting<String> INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING = new Setting<>(
        "indices.requests.cache.cleanup.staleness_threshold",
        "0%",
        IndicesRequestCache::validateStalenessSetting,
        Property.NodeScope
    );

    private final static long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Key.class);

    private final ConcurrentMap<CleanupKey, Boolean> registeredClosedListeners = ConcurrentCollections.newConcurrentMap();
    private final ByteSizeValue size;
    private final TimeValue expire;
    private final ICache<Key, BytesReference> cache;
    private final Function<ShardId, Optional<CacheEntity>> cacheEntityLookup;
    // pkg-private for testing
    final IndicesRequestCacheCleanupManager cacheCleanupManager;

    IndicesRequestCache(
        Settings settings,
        Function<ShardId, Optional<CacheEntity>> cacheEntityFunction,
        CacheService cacheService,
        ThreadPool threadPool
    ) {
        this.size = INDICES_CACHE_QUERY_SIZE.get(settings);
        this.expire = INDICES_CACHE_QUERY_EXPIRE.exists(settings) ? INDICES_CACHE_QUERY_EXPIRE.get(settings) : null;
        long sizeInBytes = size.getBytes();
        ToLongBiFunction<Key, BytesReference> weigher = (k, v) -> k.ramBytesUsed() + v.ramBytesUsed();
        this.cacheCleanupManager = new IndicesRequestCacheCleanupManager(
            threadPool,
            INDICES_REQUEST_CACHE_CLEAN_INTERVAL_SETTING.get(settings),
            getStalenessThreshold(settings)
        );
        this.cacheEntityLookup = cacheEntityFunction;
        this.cache = cacheService.createCache(
            new CacheConfig.Builder<Key, BytesReference>().setSettings(settings)
                .setWeigher(weigher)
                .setValueType(BytesReference.class)
                .setKeyType(Key.class)
                .setRemovalListener(this)
                .setMaxSizeInBytes(sizeInBytes) // for backward compatibility
                .setExpireAfterAccess(expire) // for backward compatibility
                .setCachedResultParser((bytesReference) -> {
                    try {
                        return CachedQueryResult.getPolicyValues(bytesReference);
                    } catch (IOException e) {
                        // Set took time to -1, which will always be rejected by the policy.
                        return new CachedQueryResult.PolicyValues(-1);
                    }
                })
                .setKeySerializer(new IRCKeyWriteableSerializer())
                .setValueSerializer(new BytesReferenceSerializer())
                .build(),
            CacheType.INDICES_REQUEST_CACHE
        );
    }

    @Override
    public void close() throws IOException {
        cache.invalidateAll();
        cache.close();
        cacheCleanupManager.close();
    }

    private double getStalenessThreshold(Settings settings) {
        String threshold = INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.get(settings);
        return RatioValue.parseRatioValue(threshold).getAsRatio();
    }

    void clear(CacheEntity entity) {
        cacheCleanupManager.enqueueCleanupKey(new CleanupKey(entity, null));
        cacheCleanupManager.forceCleanCache();
    }

    @Override
    public void onRemoval(RemovalNotification<Key, BytesReference> notification) {
        // In case this event happens for an old shard, we can safely ignore this as we don't keep track for old
        // shards as part of request cache.
        Key key = notification.getKey();
        cacheEntityLookup.apply(key.shardId).ifPresent(entity -> entity.onRemoval(notification));
        cacheCleanupManager.updateCleanupKeyToCountMapOnCacheEviction(
            new CleanupKey(cacheEntityLookup.apply(key.shardId).orElse(null), key.readerCacheKeyId)
        );
    }

    BytesReference getOrCompute(
        IndicesService.IndexShardCacheEntity cacheEntity,
        CheckedSupplier<BytesReference, IOException> loader,
        DirectoryReader reader,
        BytesReference cacheKey
    ) throws Exception {
        assert reader.getReaderCacheHelper() != null;
        assert reader.getReaderCacheHelper() instanceof OpenSearchDirectoryReader.DelegatingCacheHelper;

        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper = (OpenSearchDirectoryReader.DelegatingCacheHelper) reader
            .getReaderCacheHelper();
        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
        assert readerCacheKeyId != null;
        final Key key = new Key(((IndexShard) cacheEntity.getCacheIdentity()).shardId(), cacheKey, readerCacheKeyId);
        Loader cacheLoader = new Loader(cacheEntity, loader);
        BytesReference value = cache.computeIfAbsent(key, cacheLoader);
        if (cacheLoader.isLoaded()) {
            cacheEntity.onMiss();
            // see if it's the first time we see this reader, and make sure to register a cleanup key
            CleanupKey cleanupKey = new CleanupKey(cacheEntity, readerCacheKeyId);
            if (!registeredClosedListeners.containsKey(cleanupKey)) {
                Boolean previous = registeredClosedListeners.putIfAbsent(cleanupKey, Boolean.TRUE);
                if (previous == null) {
                    OpenSearchDirectoryReader.addReaderCloseListener(reader, cleanupKey);
                }
            }
            cacheCleanupManager.updateCleanupKeyToCountMapOnCacheInsertion(cleanupKey);
        } else {
            cacheEntity.onHit();
        }
        return value;
    }

    /**
     * Invalidates the given the cache entry for the given key and it's context
     * @param cacheEntity the cache entity to invalidate for
     * @param reader the reader to invalidate the cache entry for
     * @param cacheKey the cache key to invalidate
     */
    void invalidate(IndicesService.IndexShardCacheEntity cacheEntity, DirectoryReader reader, BytesReference cacheKey) {
        assert reader.getReaderCacheHelper() != null;
        String readerCacheKeyId = null;
        if (reader instanceof OpenSearchDirectoryReader) {
            IndexReader.CacheHelper cacheHelper = ((OpenSearchDirectoryReader) reader).getDelegatingCacheHelper();
            readerCacheKeyId = ((OpenSearchDirectoryReader.DelegatingCacheHelper) cacheHelper).getDelegatingCacheKey().getId();
        }
        cache.invalidate(new Key(((IndexShard) cacheEntity.getCacheIdentity()).shardId(), cacheKey, readerCacheKeyId));
    }

    /**
     * Loader for the request cache
     *
     * @opensearch.internal
     */
    private static class Loader implements LoadAwareCacheLoader<Key, BytesReference> {

        private final CacheEntity entity;
        private final CheckedSupplier<BytesReference, IOException> loader;
        private boolean loaded;

        Loader(CacheEntity entity, CheckedSupplier<BytesReference, IOException> loader) {
            this.entity = entity;
            this.loader = loader;
        }

        public boolean isLoaded() {
            return this.loaded;
        }

        @Override
        public BytesReference load(Key key) throws Exception {
            BytesReference value = loader.get();
            entity.onCached(key, value);
            loaded = true;
            return value;
        }
    }

    /**
     * Basic interface to make this cache testable.
     */
    interface CacheEntity extends Accountable {

        /**
         * Called after the value was loaded.
         */
        void onCached(Key key, BytesReference value);

        /**
         * Returns <code>true</code> iff the resource behind this entity is still open ie.
         * entities associated with it can remain in the cache. ie. IndexShard is still open.
         */
        boolean isOpen();

        /**
         * Returns the cache identity. this is, similar to {@link #isOpen()} the resource identity behind this cache entity.
         * For instance IndexShard is the identity while a CacheEntity is per DirectoryReader. Yet, we group by IndexShard instance.
         */
        Object getCacheIdentity();

        /**
         * Called each time this entity has a cache hit.
         */
        void onHit();

        /**
         * Called each time this entity has a cache miss.
         */
        void onMiss();

        /**
         * Called when this entity instance is removed
         */
        void onRemoval(RemovalNotification<Key, BytesReference> notification);

    }

    /**
     * Unique key for the cache
     *
     * @opensearch.internal
     */
    static class Key implements Accountable, Writeable {
        public final ShardId shardId; // use as identity equality
        public final String readerCacheKeyId;
        public final BytesReference value;

        Key(ShardId shardId, BytesReference value, String readerCacheKeyId) {
            this.shardId = shardId;
            this.value = value;
            this.readerCacheKeyId = Objects.requireNonNull(readerCacheKeyId);
        }

        Key(StreamInput in) throws IOException {
            this.shardId = in.readOptionalWriteable(ShardId::new);
            this.readerCacheKeyId = in.readOptionalString();
            this.value = in.readBytesReference();
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + shardId.getBaseRamBytesUsed() + value.length();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            // TODO: more detailed ram usage?
            return Collections.emptyList();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            if (!Objects.equals(readerCacheKeyId, key.readerCacheKeyId)) return false;
            if (!shardId.equals(key.shardId)) return false;
            if (!value.equals(key.value)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = shardId.hashCode();
            result = 31 * result + readerCacheKeyId.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(shardId);
            out.writeOptionalString(readerCacheKeyId);
            out.writeBytesReference(value);
        }
    }

    private class CleanupKey implements IndexReader.ClosedListener {
        final CacheEntity entity;
        final String readerCacheKeyId;

        private CleanupKey(CacheEntity entity, String readerCacheKeyId) {
            this.entity = entity;
            this.readerCacheKeyId = readerCacheKeyId;
        }

        @Override
        public void onClose(IndexReader.CacheKey cacheKey) {
            // Remove the current CleanupKey from the registeredClosedListeners map
            // If the key was present, enqueue it for cleanup
            Boolean wasRegistered = registeredClosedListeners.remove(this);
            if (wasRegistered != null) {
                cacheCleanupManager.enqueueCleanupKey(this);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CleanupKey that = (CleanupKey) o;
            if (!Objects.equals(readerCacheKeyId, that.readerCacheKeyId)) return false;
            if (!entity.getCacheIdentity().equals(that.entity.getCacheIdentity())) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = entity.getCacheIdentity().hashCode();
            result = 31 * result + Objects.hashCode(readerCacheKeyId);
            return result;
        }
    }

    /*
     * The IndicesRequestCacheCleanupManager manages the cleanup of stale keys in IndicesRequestCache.
     *
     * It also keeps track of the number of stale keys in the cache (staleKeysCount) and a staleness threshold,
     * which is used to determine when the cache should be cleaned.
     *
     * If Staleness threshold is 0, we do not keep track of stale keys in the cache
     * */
    class IndicesRequestCacheCleanupManager implements Closeable {
        private final Set<CleanupKey> keysToClean;
        private final ConcurrentMap<ShardId, HashMap<String, Integer>> cleanupKeyToCountMap;
        private final AtomicInteger staleKeysCount;
        private final double stalenessThreshold;
        private final IndicesRequestCacheCleaner cacheCleaner;

        IndicesRequestCacheCleanupManager(ThreadPool threadpool, TimeValue cleanInterval, double stalenessThreshold) {
            this.stalenessThreshold = stalenessThreshold;
            this.keysToClean = ConcurrentCollections.newConcurrentSet();
            this.cleanupKeyToCountMap = ConcurrentCollections.newConcurrentMap();
            this.staleKeysCount = new AtomicInteger(0);
            this.cacheCleaner = new IndicesRequestCacheCleaner(this, threadpool, cleanInterval);
            threadpool.schedule(cacheCleaner, cleanInterval, ThreadPool.Names.SAME);
        }

        /**
         * Enqueue cleanup key.
         *
         * @param cleanupKey the cleanup key
         */
        void enqueueCleanupKey(CleanupKey cleanupKey) {
            keysToClean.add(cleanupKey);
            incrementStaleKeysCount(cleanupKey);
        }

        /**
         * Updates the cleanupKeyToCountMap with the given CleanupKey.
         * If the ShardId associated with the CleanupKey does not exist in the map, a new entry is created.
         * The method increments the count of the CleanupKey in the map.
         * <p>
         * Why use ShardID as the key ?
         * CacheEntity mainly contains IndexShard, both of these classes do not override equals() and hashCode() methods.
         * ShardID class properly overrides equals() and hashCode() methods.
         * Therefore, to avoid modifying CacheEntity and IndexShard classes to override these methods, we use ShardID as the key.
         *
         * @param cleanupKey the CleanupKey to be updated in the map
         */
        private void updateCleanupKeyToCountMapOnCacheInsertion(CleanupKey cleanupKey) {
            if (stalenessThreshold == 0.0 || cleanupKey.entity == null) {
                return;
            }
            IndexShard indexShard = (IndexShard) cleanupKey.entity.getCacheIdentity();
            if (indexShard == null) {
                logger.warn("IndexShard is null for CleanupKey: {} while cleaning Indices Request Cache", cleanupKey.readerCacheKeyId);
                return;
            }
            ShardId shardId = indexShard.shardId();

            // If the key doesn't exist, it's added with a value of 1.
            // If the key exists, its value is incremented by 1.
            cleanupKeyToCountMap.computeIfAbsent(shardId, k -> new HashMap<>()).merge(cleanupKey.readerCacheKeyId, 1, Integer::sum);
        }

        private void updateCleanupKeyToCountMapOnCacheEviction(CleanupKey cleanupKey) {
            if (stalenessThreshold == 0.0 || cleanupKey.entity == null) {
                return;
            }
            IndexShard indexShard = (IndexShard) cleanupKey.entity.getCacheIdentity();
            if (indexShard == null) {
                logger.warn("IndexShard is null for CleanupKey: {} while cleaning Indices Request Cache", cleanupKey.readerCacheKeyId);
                return;
            }
            ShardId shardId = indexShard.shardId();

            cleanupKeyToCountMap.computeIfPresent(shardId, (shard, keyCountMap) -> {
                keyCountMap.computeIfPresent(cleanupKey.readerCacheKeyId, (key, currentValue) -> {
                    // decrement the stale key count
                    staleKeysCount.decrementAndGet();
                    int newValue = currentValue - 1;
                    // Remove the key if the new value is zero by returning null; otherwise, update with the new value.
                    return newValue == 0 ? null : newValue;
                });
                return keyCountMap;
            });
        }

        /**
         * Updates the count of stale keys in the cache.
         * This method is called when a CleanupKey is added to the keysToClean set.
         *
         * It increments the staleKeysCount by the count of the CleanupKey in the cleanupKeyToCountMap.
         * If the CleanupKey's readerCacheKeyId is null or the CleanupKey's entity is not open, it increments the staleKeysCount
         * by the total count of keys associated with the CleanupKey's ShardId in the cleanupKeyToCountMap and removes the ShardId from the map.
         *
         * @param cleanupKey the CleanupKey that has been marked for cleanup
         */
        private void incrementStaleKeysCount(CleanupKey cleanupKey) {
            if (stalenessThreshold == 0.0 || cleanupKey.entity == null) {
                return;
            }
            IndexShard indexShard = (IndexShard) cleanupKey.entity.getCacheIdentity();
            if (indexShard == null) {
                logger.warn("IndexShard is null for CleanupKey: {}", cleanupKey.readerCacheKeyId);
                return;
            }
            ShardId shardId = indexShard.shardId();

            // Using computeIfPresent to atomically operate on the countMap for a given shardId
            cleanupKeyToCountMap.computeIfPresent(shardId, (key, countMap) -> {
                if (cleanupKey.readerCacheKeyId == null) {
                    // Aggregate and add to staleKeysCount atomically if readerCacheKeyId is null
                    int totalSum = countMap.values().stream().mapToInt(Integer::intValue).sum();
                    staleKeysCount.addAndGet(totalSum);
                    // Return null to automatically remove the mapping for shardId
                    return null;
                } else {
                    // Update staleKeysCount based on specific readerCacheKeyId, then remove it from the countMap
                    countMap.computeIfPresent(cleanupKey.readerCacheKeyId, (k, v) -> {
                        staleKeysCount.addAndGet(v);
                        // Return null to remove the key after updating staleKeysCount
                        return null;
                    });

                    // Check if countMap is empty after removal to decide if we need to remove the shardId entry
                    if (countMap.isEmpty()) {
                        return null; // Returning null removes the entry for shardId
                    }
                }
                return countMap; // Return the modified countMap to keep the mapping
            });
        }

        // package private for testing
        AtomicInteger getStaleKeysCount() {
            return staleKeysCount;
        }

        /**
         * Clean cache based on stalenessThreshold
         */
        void cleanCache() {
            cleanCache(stalenessThreshold);
        }

        /**
         * Force Clean cache without checking stalenessThreshold
         */
        private void forceCleanCache() {
            cleanCache(0);
        }

        /**
         * Cleans the cache based on the provided staleness threshold.
         * <p>If the percentage of stale keys in the cache is less than this threshold,the cache cleanup process is skipped.
         * @param stalenessThreshold The staleness threshold as a double.
         */
        private synchronized void cleanCache(double stalenessThreshold) {
            if (logger.isDebugEnabled()) {
                logger.debug("Cleaning Indices Request Cache with threshold : " + stalenessThreshold);
            }
            if (canSkipCacheCleanup(stalenessThreshold)) {
                return;
            }
            // Contains CleanupKey objects with open shard but invalidated readerCacheKeyId.
            final Set<CleanupKey> cleanupKeysFromOutdatedReaders = new HashSet<>();
            // Contains CleanupKey objects of a closed shard.
            final Set<Object> cleanupKeysFromClosedShards = new HashSet<>();

            for (Iterator<CleanupKey> iterator = keysToClean.iterator(); iterator.hasNext();) {
                CleanupKey cleanupKey = iterator.next();
                iterator.remove();
                if (cleanupKey.readerCacheKeyId == null || !cleanupKey.entity.isOpen()) {
                    // null indicates full cleanup, as does a closed shard
                    cleanupKeysFromClosedShards.add(((IndexShard) cleanupKey.entity.getCacheIdentity()).shardId());
                } else {
                    cleanupKeysFromOutdatedReaders.add(cleanupKey);
                }
            }

            if (cleanupKeysFromOutdatedReaders.isEmpty() && cleanupKeysFromClosedShards.isEmpty()) {
                return;
            }

            for (Iterator<Key> iterator = cache.keys().iterator(); iterator.hasNext();) {
                Key key = iterator.next();
                if (cleanupKeysFromClosedShards.contains(key.shardId)) {
                    iterator.remove();
                } else {
                    CleanupKey cleanupKey = new CleanupKey(cacheEntityLookup.apply(key.shardId).orElse(null), key.readerCacheKeyId);
                    if (cleanupKeysFromOutdatedReaders.contains(cleanupKey)) {
                        iterator.remove();
                    }
                }
            }
            cache.refresh();
        }

        /**
         * Determines whether the cache cleanup process can be skipped based on the staleness threshold.
         *
         * <p>If the percentage of stale keys is less than the provided staleness threshold returns true,
         * indicating that the cache cleanup process can be skipped.
         *
         * @param cleanThresholdPercent The staleness threshold as a percentage.
         * @return true if the cache cleanup process can be skipped, false otherwise.
         */
        private synchronized boolean canSkipCacheCleanup(double cleanThresholdPercent) {
            if (cleanThresholdPercent == 0.0) {
                return false;
            }
            double staleKeysInCachePercentage = staleKeysInCachePercentage();
            if (staleKeysInCachePercentage < cleanThresholdPercent) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Skipping Indices Request cache cleanup since the percentage of stale keys : "
                            + staleKeysInCachePercentage
                            + " is less than the threshold : "
                            + stalenessThreshold
                    );
                }
                return true;
            }
            return false;
        }

        /**
         * Calculates and returns the percentage of stale keys in the cache.
         *
         * @return The percentage of stale keys in the cache as a double. Returns 0 if there are no keys in the cache or no stale keys.
         */
        private synchronized double staleKeysInCachePercentage() {
            long totalKeysInCache = count();
            if (totalKeysInCache == 0 || staleKeysCount.get() == 0) {
                return 0;
            }
            return ((double) staleKeysCount.get() / totalKeysInCache);
        }

        @Override
        public void close() {
            this.cacheCleaner.close();
        }

        private final class IndicesRequestCacheCleaner implements Runnable, Releasable {

            private final IndicesRequestCacheCleanupManager cacheCleanupManager;
            private final ThreadPool threadPool;
            private final TimeValue interval;

            IndicesRequestCacheCleaner(IndicesRequestCacheCleanupManager cacheCleanupManager, ThreadPool threadPool, TimeValue interval) {
                this.cacheCleanupManager = cacheCleanupManager;
                this.threadPool = threadPool;
                this.interval = interval;
            }

            private final AtomicBoolean closed = new AtomicBoolean(false);

            @Override
            public void run() {
                try {
                    this.cacheCleanupManager.cleanCache();
                } catch (Exception e) {
                    logger.warn("Exception during periodic indices request cache cleanup:", e);
                }
                // Reschedule itself to run again if not closed
                if (closed.get() == false) {
                    threadPool.scheduleUnlessShuttingDown(interval, ThreadPool.Names.SAME, this);
                }
            }

            @Override
            public void close() {
                closed.compareAndSet(false, true);
            }
        }
    }

    /**
     * Returns the current size of the cache
     */
    long count() {
        return cache.count();
    }

    int numRegisteredCloseListeners() { // for testing
        return registeredClosedListeners.size();
    }

    /**
     * Validates the staleness setting for the cache cleanup threshold.
     *
     * <p>This method checks if the provided staleness threshold is a valid percentage or a valid double value.
     * If the staleness threshold is not valid, it throws an OpenSearchParseException.
     *
     * @param staleThreshold The staleness threshold to validate.
     * @return The validated staleness threshold.
     * @throws OpenSearchParseException If the staleness threshold is not a valid percentage or double value.
     *
     * <p>package private for testing
     */
    static String validateStalenessSetting(String staleThreshold) {
        try {
            RatioValue.parseRatioValue(staleThreshold);
        } catch (OpenSearchParseException e) {
            e.addSuppressed(e);
            throw e;
        }
        return staleThreshold;
    }
}
