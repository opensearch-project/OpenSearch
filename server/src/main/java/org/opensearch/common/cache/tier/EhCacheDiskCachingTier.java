/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.serialization.SerializerException;
import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
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

/**
 * @param <K> The key type of cache entries
 * @param <V> The value type of cache entries
 */
public class EhCacheDiskCachingTier<K, V> implements DiskCachingTier<K, V> {

    // A Cache manager can create many caches.
    private static PersistentCacheManager cacheManager = null;

    // Disk cache
    private Cache<K, byte[]> cache;
    private final long maxWeightInBytes;
    private final String storagePath;

    private final Class<K> keyType;

    private final Class<V> valueType;

    private final TimeValue expireAfterAccess;

    private final EhCacheEventListener<K, V> ehCacheEventListener;

    private final String threadPoolAlias;

    private final Settings settings;

    private CounterMetric count = new CounterMetric();

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
    // will hold that many file pointers.
    public final Setting<Integer> DISK_SEGMENTS;

    private final Serializer<K, byte[]> keySerializer;
    private final Serializer<V, byte[]> valueSerializer;

    private EhCacheDiskCachingTier(Builder<K, V> builder) {
        this.keyType = Objects.requireNonNull(builder.keyType, "Key type shouldn't be null");
        this.valueType = Objects.requireNonNull(builder.valueType, "Value type shouldn't be null");
        this.expireAfterAccess = Objects.requireNonNull(builder.expireAfterAcess, "ExpireAfterAccess value shouldn't " + "be null");
        this.keySerializer = Objects.requireNonNull(builder.keySerializer, "Key serializer shouldn't be null");
        this.valueSerializer = Objects.requireNonNull(builder.valueSerializer, "Value serializer shouldn't be null");
        this.ehCacheEventListener = new EhCacheEventListener<K, V>(this.valueSerializer);
        this.maxWeightInBytes = builder.maxWeightInBytes;
        this.storagePath = Objects.requireNonNull(builder.storagePath, "Storage path shouldn't be null");
        if (builder.threadPoolAlias == null || builder.threadPoolAlias.isBlank()) {
            this.threadPoolAlias = THREAD_POOL_ALIAS_PREFIX + "DiskWrite";
        } else {
            this.threadPoolAlias = builder.threadPoolAlias;
        }
        this.settings = Objects.requireNonNull(builder.settings, "Settings objects shouldn't be null");
        Objects.requireNonNull(builder.settingPrefix, "Setting prefix shouldn't be null");
        this.DISK_WRITE_MINIMUM_THREADS = Setting.intSetting(builder.settingPrefix + ".tiered.disk.ehcache.min_threads", 2, 1, 5);
        this.DISK_WRITE_MAXIMUM_THREADS = Setting.intSetting(builder.settingPrefix + ".tiered.disk.ehcache.max_threads", 2, 1, 20);
        // Default value is 1 within EhCache.
        this.DISK_WRITE_CONCURRENCY = Setting.intSetting(builder.settingPrefix + ".tiered.disk.ehcache.concurrency", 2, 1, 3);
        // Default value is 16 within Ehcache.
        this.DISK_SEGMENTS = Setting.intSetting(builder.settingPrefix + ".ehcache.disk.segments", 16, 1, 32);
        if (cacheManager == null) {
            cacheManager = buildCacheManager();
            this.cache = buildCache(Duration.ofMillis(expireAfterAccess.getMillis()), builder);
        }

        // IndicesRequestCache gets 1%, of which we allocate 5% to the keystore = 0.05%
        // TODO: how do we change this automatically based on INDICES_CACHE_QUERY_SIZE setting?
        //Setting<ByteSizeValue> keystoreSizeSetting = Setting.memorySizeSetting(builder.settingPrefix + ".tiered.disk.keystore_size", "0.05%");
        //this.keystore = new RBMIntKeyLookupStore(keystoreSizeSetting.get(this.settings).getBytes());
        long keystoreMaxWeight = builder.keystoreMaxWeightInBytes;
        this.keystore = new RBMIntKeyLookupStore(keystoreMaxWeight);
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

    private Cache<K, byte[]> buildCache(Duration expireAfterAccess, Builder<K, V> builder) {
        return cacheManager.createCache(
            DISK_CACHE_ALIAS,
            CacheConfigurationBuilder.newCacheConfigurationBuilder(
                keyType,
                byte[].class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B)
            ).withExpiry(new ExpiryPolicy<K, byte[]>() {
                @Override
                public Duration getExpiryForCreation(K key, byte[] value) {
                    return INFINITE;
                }

                @Override
                public Duration getExpiryForAccess(K key, Supplier<? extends byte[]> value) {
                    return expireAfterAccess;
                }

                @Override
                public Duration getExpiryForUpdate(K key, Supplier<? extends byte[]> oldValue, byte[] newValue) {
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
                .withKeySerializer(new KeySerializerWrapper<K>(keySerializer))
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

    @Override
    public V get(K key) {
        // Optimize it by adding key store.
        return valueSerializer.deserialize(cache.get(key));
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, valueSerializer.serialize(value));
    }

    @Override
    public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
        // Ehcache doesn't offer any such function. Will have to implement our own if needed later on.
        throw new UnsupportedOperationException();
    }

    @Override
    public void invalidate(K key) {
        // There seems to be a thread leak issue while calling this and then closing cache.
        cache.remove(key);
    }

    @Override
    public V compute(K key, TieredCacheLoader<K, V> loader) throws Exception {
        // Ehcache doesn't offer any such function. Will have to implement our own if needed later on.
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRemovalListener(RemovalListener<K, V> removalListener) {
        ehCacheEventListener.setRemovalListener(removalListener);
    }

    @Override
    public void invalidateAll() {
        // Clear up files.
    }

    @Override
    public Iterable<K> keys() {
        return () -> new EhCacheKeyIterator<>(cache.iterator());
    }

    @Override
    public int count() {
        return (int) count.count();
    }

    @Override
    public TierType getTierType() {
        return TierType.DISK;
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

    /**
     * Wrapper over Ehcache original listener to listen to desired events and notify desired subscribers.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    class EhCacheEventListener<K, V> implements CacheEventListener<K, byte[]> {

        private Optional<RemovalListener<K, V>> removalListener;
        // We need to pass the value serializer to this listener, as the removal listener is expecting
        // values of type K, V, not K, byte[]
        private Serializer<V, byte[]> valueSerializer;

        EhCacheEventListener(Serializer<V, byte[]> valueSerializer) {
            this.valueSerializer = valueSerializer;
        }

        public void setRemovalListener(RemovalListener<K, V> removalListener) {
            this.removalListener = Optional.ofNullable(removalListener);
        }

        @Override
        public void onEvent(CacheEvent<? extends K, ? extends byte[]> event) {
            switch (event.getType()) {
                case CREATED:
                    count.inc();
                    assert event.getOldValue() == null;
                    break;
                case EVICTED:
                    this.removalListener.ifPresent(
                        listener -> listener.onRemoval(
                            new RemovalNotification<>(
                                event.getKey(),
                                valueSerializer.deserialize(event.getOldValue()),
                                RemovalReason.EVICTED,
                                TierType.DISK
                            )
                        )
                    );
                    count.dec();
                    assert event.getNewValue() == null;
                    break;
                case REMOVED:
                    count.dec();
                    assert event.getNewValue() == null;
                    break;
                case EXPIRED:
                    this.removalListener.ifPresent(
                        listener -> listener.onRemoval(
                            new RemovalNotification<>(
                                event.getKey(),
                                valueSerializer.deserialize(event.getOldValue()),
                                RemovalReason.INVALIDATED,
                                TierType.DISK
                            )
                        )
                    );
                    count.dec();
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

        Iterator<Cache.Entry<K, byte[]>> iterator;

        EhCacheKeyIterator(Iterator<Cache.Entry<K, byte[]>> iterator) {
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
     * The wrapper for the key serializer which is passed directly to Ehcache.
     */
    private class KeySerializerWrapper<K> implements org.ehcache.spi.serialization.Serializer<K> {
        public Serializer<K, byte[]> serializer;
        public KeySerializerWrapper(Serializer<K, byte[]> serializer) {
            this.serializer = serializer;
        }

        // This constructor must be present, but does not have to work as we are not actually persisting the disk
        // cache after a restart.
        // See https://www.ehcache.org/documentation/3.0/serializers-copiers.html#persistent-vs-transient-caches
        public KeySerializerWrapper(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {}

        @Override
        public ByteBuffer serialize(K object) throws SerializerException {
            return ByteBuffer.wrap(serializer.serialize(object));
        }

        @Override
        public K read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            byte[] arr = new byte[binary.remaining()];
            binary.get(arr);
            return serializer.deserialize(arr);
        }

        @Override
        public boolean equals(K object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            byte[] arr = new byte[binary.remaining()];
            binary.get(arr);
            return serializer.equals(object, arr);
        }
    }

    /**
     * Builder object to build Ehcache disk tier.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> {
        private long maxWeightInBytes;
        private TimeValue expireAfterAcess;

        private Class<K> keyType;

        private Class<V> valueType;

        private String storagePath;

        private String threadPoolAlias;

        private Settings settings;

        private String diskCacheAlias;

        private String settingPrefix;

        // Provides capability to make ehCache event listener to run in sync mode. Used for testing too.
        private boolean isEventListenerModeSync;
        private Serializer<K, byte[]> keySerializer;
        private Serializer<V, byte[]> valueSerializer;
        private long keystoreMaxWeightInBytes = 0;

        public Builder() {}

        public EhCacheDiskCachingTier.Builder<K, V> setMaximumWeightInBytes(long sizeInBytes) {
            if (sizeInBytes <= MINIMUM_MAX_SIZE_IN_BYTES) {
                throw new IllegalArgumentException("Ehcache Disk tier cache size should be greater than " + MINIMUM_MAX_SIZE_IN_BYTES);
            }
            this.maxWeightInBytes = sizeInBytes;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setExpireAfterAccess(TimeValue expireAfterAcess) {
            this.expireAfterAcess = expireAfterAcess;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setKeyType(Class<K> keyType) {
            this.keyType = keyType;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setValueType(Class<V> valueType) {
            this.valueType = valueType;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setStoragePath(String storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setThreadPoolAlias(String threadPoolAlias) {
            this.threadPoolAlias = threadPoolAlias;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setDiskCacheAlias(String diskCacheAlias) {
            this.diskCacheAlias = diskCacheAlias;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setSettingPrefix(String settingPrefix) {
            // TODO: Do some basic validation. So that it doesn't end with "." etc.
            this.settingPrefix = settingPrefix;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setIsEventListenerModeSync(boolean isEventListenerModeSync) {
            this.isEventListenerModeSync = isEventListenerModeSync;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setKeySerializer(Serializer<K, byte[]> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setValueSerializer(Serializer<V, byte[]> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public EhCacheDiskCachingTier.Builder<K, V> setKeyStoreMaxWeightInBytes(long weight) {
            this.keystoreMaxWeightInBytes = weight;
            return this;
        }

        public EhCacheDiskCachingTier<K, V> build() {
            return new EhCacheDiskCachingTier<>(this);
        }
    }
}
