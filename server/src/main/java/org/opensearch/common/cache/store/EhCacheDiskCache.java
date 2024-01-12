/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.store.builders.StoreAwareCacheBuilder;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.io.File;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.ehcache.Cache;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;

/**
 * This variant of disk cache uses Ehcache underneath.
 *  @param <K> Type of key.
 *  @param <V> Type of value.
 *
 *  @opensearch.experimental
 *
 */
public class EhCacheDiskCache<K, V> implements StoreAwareCache<K, V> {

    private static final Logger logger = LogManager.getLogger(EhCacheDiskCache.class);

    // A Cache manager can create many caches.
    private final PersistentCacheManager cacheManager;

    // Disk cache
    private Cache<K, V> cache;
    private final long maxWeightInBytes;
    private final String storagePath;

    private final Class<K> keyType;

    private final Class<V> valueType;

    private final TimeValue expireAfterAccess;

    private final DiskCacheStats stats = new DiskCacheStats();

    private final EhCacheEventListener<K, V> ehCacheEventListener;

    private final String threadPoolAlias;

    private final Settings settings;

    private final static String DISK_CACHE_ALIAS = "ehDiskCache";

    private final static String THREAD_POOL_ALIAS_PREFIX = "ehcachePool";

    private final static int MINIMUM_MAX_SIZE_IN_BYTES = 1024 * 100; // 100KB

    // Ehcache disk write minimum threads for its pool
    public final Setting<Integer> DISK_WRITE_MINIMUM_THREADS;

    // Ehcache disk write maximum threads for its pool
    public final Setting<Integer> DISK_WRITE_MAXIMUM_THREADS;

    // Not be to confused with number of disk segments, this is different. Defines
    // distinct write queues created for disk store where a group of segments share a write queue. This is
    // implemented with ehcache using a partitioned thread pool exectutor By default all segments share a single write
    // queue ie write concurrency is 1. Check OffHeapDiskStoreConfiguration and DiskWriteThreadPool.
    public final Setting<Integer> DISK_WRITE_CONCURRENCY;

    // Defines how many segments the disk cache is separated into. Higher number achieves greater concurrency but
    // will hold that many file pointers. Default is 16.
    public final Setting<Integer> DISK_SEGMENTS;

    private final StoreAwareCacheEventListener<K, V> eventListener;

    /**
     * Used in computeIfAbsent to synchronize loading of a given key. This is needed as ehcache doesn't provide a
     * computeIfAbsent method.
     */
    Map<K, CompletableFuture<Tuple<K, V>>> completableFutureMap = new ConcurrentHashMap<>();

    private EhCacheDiskCache(Builder<K, V> builder) {
        this.keyType = Objects.requireNonNull(builder.keyType, "Key type shouldn't be null");
        this.valueType = Objects.requireNonNull(builder.valueType, "Value type shouldn't be null");
        this.expireAfterAccess = Objects.requireNonNull(builder.getExpireAfterAcess(), "ExpireAfterAccess value shouldn't " + "be null");
        this.maxWeightInBytes = builder.getMaxWeightInBytes();
        if (this.maxWeightInBytes <= MINIMUM_MAX_SIZE_IN_BYTES) {
            throw new IllegalArgumentException("Ehcache Disk tier cache size should be greater than " + MINIMUM_MAX_SIZE_IN_BYTES);
        }
        this.storagePath = Objects.requireNonNull(builder.storagePath, "Storage path shouldn't be null");
        if (builder.threadPoolAlias == null || builder.threadPoolAlias.isBlank()) {
            this.threadPoolAlias = THREAD_POOL_ALIAS_PREFIX + "DiskWrite";
        } else {
            this.threadPoolAlias = builder.threadPoolAlias;
        }
        this.settings = Objects.requireNonNull(builder.settings, "Settings objects shouldn't be null");
        Objects.requireNonNull(builder.settingPrefix, "Setting prefix shouldn't be null");
        this.DISK_WRITE_MINIMUM_THREADS = Setting.intSetting(builder.settingPrefix + ".tier.disk.ehcache.min_threads", 2, 1, 5);
        this.DISK_WRITE_MAXIMUM_THREADS = Setting.intSetting(builder.settingPrefix + ".tier.disk.ehcache.max_threads", 2, 1, 20);
        // Default value is 1 within EhCache.
        this.DISK_WRITE_CONCURRENCY = Setting.intSetting(builder.settingPrefix + ".tier.disk.ehcache.concurrency", 2, 1, 3);
        // Default value is 16 within Ehcache.
        this.DISK_SEGMENTS = Setting.intSetting(builder.settingPrefix + "tier.disk.ehcache.segments", 16, 1, 32);
        this.cacheManager = buildCacheManager();
        Objects.requireNonNull(builder.getEventListener(), "Listener can't be null");
        this.eventListener = builder.getEventListener();
        this.ehCacheEventListener = new EhCacheEventListener<K, V>(builder.getEventListener());
        this.cache = buildCache(Duration.ofMillis(expireAfterAccess.getMillis()), builder);
    }

    private PersistentCacheManager buildCacheManager() {
        // In case we use multiple ehCaches, we can define this cache manager at a global level.
        return CacheManagerBuilder.newCacheManagerBuilder()
            .with(CacheManagerBuilder.persistence(new File(storagePath)))
            .using(
                PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder()
                    .defaultPool(THREAD_POOL_ALIAS_PREFIX + "Default", 1, 3) // Default pool used for other tasks like
                    // event listeners
                    .pool(this.threadPoolAlias, DISK_WRITE_MINIMUM_THREADS.get(settings), DISK_WRITE_MAXIMUM_THREADS.get(settings))
                    .build()
            )
            .build(true);
    }

    private Cache<K, V> buildCache(Duration expireAfterAccess, Builder<K, V> builder) {
        return this.cacheManager.createCache(
            DISK_CACHE_ALIAS,
            CacheConfigurationBuilder.newCacheConfigurationBuilder(
                this.keyType,
                this.valueType,
                ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B)
            ).withExpiry(new ExpiryPolicy<>() {
                @Override
                public Duration getExpiryForCreation(K key, V value) {
                    return INFINITE;
                }

                @Override
                public Duration getExpiryForAccess(K key, Supplier<? extends V> value) {
                    return expireAfterAccess;
                }

                @Override
                public Duration getExpiryForUpdate(K key, Supplier<? extends V> oldValue, V newValue) {
                    return INFINITE;
                }
            })
                .withService(getListenerConfiguration(builder))
                .withService(
                    new OffHeapDiskStoreConfiguration(
                        this.threadPoolAlias,
                        DISK_WRITE_CONCURRENCY.get(settings),
                        DISK_SEGMENTS.get(settings)
                    )
                )
        );
    }

    private CacheEventListenerConfigurationBuilder getListenerConfiguration(Builder<K, V> builder) {
        CacheEventListenerConfigurationBuilder configurationBuilder = CacheEventListenerConfigurationBuilder.newEventListenerConfiguration(
            this.ehCacheEventListener,
            EventType.EVICTED,
            EventType.EXPIRED,
            EventType.REMOVED,
            EventType.UPDATED,
            EventType.CREATED
        ).unordered();
        if (builder.isEventListenerModeSync) {
            return configurationBuilder.synchronous();
        } else {
            return configurationBuilder.asynchronous();
        }
    }

    // Package private for testing
    Map<K, CompletableFuture<Tuple<K, V>>> getCompletableFutureMap() {
        return completableFutureMap;
    }

    @Override
    public V get(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key passed to ehcache disk cache was null.");
        }
        // Optimize it by adding key store.
        V value;
        try {
            value = cache.get(key);
        } catch (CacheLoadingException ex) {
            throw new OpenSearchException("Exception occurred while trying to fetch item from ehcache disk cache");
        }
        if (value != null) {
            eventListener.onHit(key, value, CacheStoreType.DISK);
        } else {
            eventListener.onMiss(key, CacheStoreType.DISK);
        }
        return value;
    }

    @Override
    public void put(K key, V value) {
        try {
            cache.put(key, value);
        } catch (CacheWritingException ex) {
            throw new OpenSearchException("Exception occurred while put item to ehcache disk cache");
        }
    }

    @Override
    public V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        // Ehache doesn't provide any computeIfAbsent function. Exposes putIfAbsent but that works differently and is
        // not performant in case there are multiple concurrent request for same key. Below is our own custom
        // implementation of computeIfAbsent on top of ehcache. Inspired by OpenSearch Cache implementation.
        V value = cache.get(key);
        if (value == null) {
            value = compute(key, loader);
        }
        if (!loader.isLoaded()) {
            eventListener.onHit(key, value, CacheStoreType.DISK);
        } else {
            eventListener.onMiss(key, CacheStoreType.DISK);
        }
        return value;
    }

    private V compute(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        // A future that returns a pair of key/value.
        CompletableFuture<Tuple<K, V>> completableFuture = new CompletableFuture<>();
        // Only one of the threads will succeed putting a future into map for the same key.
        // Rest will fetch existing future.
        CompletableFuture<Tuple<K, V>> future = completableFutureMap.putIfAbsent(key, completableFuture);
        // Handler to handle results post processing. Takes a tuple<key, value> or exception as an input and returns
        // the value. Also before returning value, puts the value in cache.
        BiFunction<Tuple<K, V>, Throwable, V> handler = (pair, ex) -> {
            V value = null;
            if (pair != null) {
                cache.put(pair.v1(), pair.v2());
                value = pair.v2(); // Returning a value itself assuming that a next get should return the same. Should
                // be safe to assume if we got no exception and reached here.
            }
            completableFutureMap.remove(key); // Remove key from map as not needed anymore.
            return value;
        };
        CompletableFuture<V> completableValue;
        if (future == null) {
            future = completableFuture;
            completableValue = future.handle(handler);
            V value;
            try {
                value = loader.load(key);
            } catch (Exception ex) {
                future.completeExceptionally(ex);
                throw new ExecutionException(ex);
            }
            if (value == null) {
                NullPointerException npe = new NullPointerException("loader returned a null value");
                future.completeExceptionally(npe);
                throw new ExecutionException(npe);
            } else {
                future.complete(new Tuple<>(key, value));
            }

        } else {
            completableValue = future.handle(handler);
        }
        V value;
        try {
            value = completableValue.get();
            if (future.isCompletedExceptionally()) {
                future.get(); // call get to force the exception to be thrown for other concurrent callers
                throw new IllegalStateException("Future completed exceptionally but no error thrown");
            }
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
        return value;
    }

    @Override
    public void invalidate(K key) {
        // There seems to be an thread leak issue while calling this and then closing cache.
        try {
            cache.remove(key);
        } catch (CacheWritingException ex) {
            // Handle
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void invalidateAll() {
        // TODO
    }

    @Override
    public Iterable<K> keys() {
        return () -> new EhCacheKeyIterator<>(cache.iterator());
    }

    @Override
    public long count() {
        return stats.count();
    }

    @Override
    public void refresh() {
        // TODO: ehcache doesn't provide a way to refresh a cache.
    }

    @Override
    public CacheStoreType getTierType() {
        return CacheStoreType.DISK;
    }

    @Override
    public void close() {
        cacheManager.removeCache(DISK_CACHE_ALIAS);
        cacheManager.close();
        try {
            cacheManager.destroyCache(DISK_CACHE_ALIAS);
        } catch (CachePersistenceException e) {
            throw new OpenSearchException("Exception occurred while destroying ehcache and associated data", e);
        }
    }

    @Override
    public CacheStats stats() {
        return stats;
    }

    /**
     * Stats related to disk cache.
     */
    class DiskCacheStats implements CacheStats {
        private CounterMetric count = new CounterMetric();

        @Override
        public long count() {
            return count.count();
        }
    }

    /**
     * Wrapper over Ehcache original listener to listen to desired events and notify desired subscribers.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    class EhCacheEventListener<K, V> implements CacheEventListener<K, V> {

        private final StoreAwareCacheEventListener<K, V> eventListener;

        EhCacheEventListener(StoreAwareCacheEventListener<K, V> eventListener) {
            this.eventListener = eventListener;
        }

        @Override
        public void onEvent(CacheEvent<? extends K, ? extends V> event) {
            switch (event.getType()) {
                case CREATED:
                    stats.count.inc();
                    this.eventListener.onCached(event.getKey(), event.getNewValue(), CacheStoreType.DISK);
                    assert event.getOldValue() == null;
                    break;
                case EVICTED:
                    this.eventListener.onRemoval(
                        new StoreAwareCacheRemovalNotification<>(
                            event.getKey(),
                            event.getOldValue(),
                            RemovalReason.EVICTED,
                            CacheStoreType.DISK
                        )
                    );
                    stats.count.dec();
                    assert event.getNewValue() == null;
                    break;
                case REMOVED:
                    stats.count.dec();
                    this.eventListener.onRemoval(
                        new StoreAwareCacheRemovalNotification<>(
                            event.getKey(),
                            event.getOldValue(),
                            RemovalReason.EXPLICIT,
                            CacheStoreType.DISK
                        )
                    );
                    assert event.getNewValue() == null;
                    break;
                case EXPIRED:
                    this.eventListener.onRemoval(
                        new StoreAwareCacheRemovalNotification<>(
                            event.getKey(),
                            event.getOldValue(),
                            RemovalReason.INVALIDATED,
                            CacheStoreType.DISK
                        )
                    );
                    stats.count.dec();
                    assert event.getNewValue() == null;
                    break;
                case UPDATED:
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * This iterator wraps ehCache iterator and only iterates over its keys.
     * @param <K> Type of key
     */
    class EhCacheKeyIterator<K> implements Iterator<K> {

        Iterator<Cache.Entry<K, V>> iterator;

        EhCacheKeyIterator(Iterator<Cache.Entry<K, V>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public K next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next().getKey();
        }
    }

    /**
     * Builder object to build Ehcache disk tier.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> extends StoreAwareCacheBuilder<K, V> {
        private Class<K> keyType;

        private Class<V> valueType;

        private String storagePath;

        private String threadPoolAlias;

        private Settings settings;

        private String diskCacheAlias;

        private String settingPrefix;

        // Provides capability to make ehCache event listener to run in sync mode. Used for testing too.
        private boolean isEventListenerModeSync;

        public Builder() {}

        public EhCacheDiskCache.Builder<K, V> setKeyType(Class<K> keyType) {
            this.keyType = keyType;
            return this;
        }

        public EhCacheDiskCache.Builder<K, V> setValueType(Class<V> valueType) {
            this.valueType = valueType;
            return this;
        }

        public EhCacheDiskCache.Builder<K, V> setStoragePath(String storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        public EhCacheDiskCache.Builder<K, V> setThreadPoolAlias(String threadPoolAlias) {
            this.threadPoolAlias = threadPoolAlias;
            return this;
        }

        public EhCacheDiskCache.Builder<K, V> setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public EhCacheDiskCache.Builder<K, V> setDiskCacheAlias(String diskCacheAlias) {
            this.diskCacheAlias = diskCacheAlias;
            return this;
        }

        public EhCacheDiskCache.Builder<K, V> setSettingPrefix(String settingPrefix) {
            // TODO: Do some basic validation. So that it doesn't end with "." etc.
            this.settingPrefix = settingPrefix;
            return this;
        }

        public EhCacheDiskCache.Builder<K, V> setIsEventListenerModeSync(boolean isEventListenerModeSync) {
            this.isEventListenerModeSync = isEventListenerModeSync;
            return this;
        }

        public EhCacheDiskCache<K, V> build() {
            return new EhCacheDiskCache<>(this);
        }
    }
}
