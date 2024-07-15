/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store.disk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.cache.EhcacheDiskCacheSettings;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.serializer.ICacheKeySerializer;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.cache.stats.CacheStatsHolder;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.stats.NoopCacheStatsHolder;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.spi.serialization.SerializerException;

import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_CACHE_ALIAS_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_CACHE_EXPIRE_AFTER_ACCESS_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_LISTENER_MODE_SYNC_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_MAX_SIZE_IN_BYTES_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_SEGMENT_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_STORAGE_PATH_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_WRITE_CONCURRENCY_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_WRITE_MAXIMUM_THREADS_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_WRITE_MIN_THREADS_KEY;

/**
 * This variant of disk cache uses Ehcache underneath.
 *  @param <K> Type of key.
 *  @param <V> Type of value.
 *
 *  @opensearch.experimental
 *
 */
@ExperimentalApi
public class EhcacheDiskCache<K, V> implements ICache<K, V> {

    private static final Logger logger = LogManager.getLogger(EhcacheDiskCache.class);

    // Unique id associated with this cache.
    private final static String UNIQUE_ID = UUID.randomUUID().toString();
    private final static String THREAD_POOL_ALIAS_PREFIX = "ehcachePool";
    // A Cache manager can create many caches.
    private final PersistentCacheManager cacheManager;

    // Disk cache. Using ByteArrayWrapper to compare two byte[] by values rather than the default reference checks
    @SuppressWarnings({ "rawtypes" }) // We have to use the raw type as there's no way to pass the "generic class" to ehcache
    private Cache<ICacheKey, ByteArrayWrapper> cache;
    private final long maxWeightInBytes;
    private final String storagePath;
    private final Class<K> keyType;
    private final Class<V> valueType;
    private final TimeValue expireAfterAccess;
    private final CacheStatsHolder cacheStatsHolder;
    private final EhCacheEventListener ehCacheEventListener;
    private final String threadPoolAlias;
    private final Settings settings;
    private final RemovalListener<ICacheKey<K>, V> removalListener;
    private final CacheType cacheType;
    private final String diskCacheAlias;
    private final Serializer<K, byte[]> keySerializer;
    private final Serializer<V, byte[]> valueSerializer;

    final static int MINIMUM_MAX_SIZE_IN_BYTES = 1024 * 100; // 100KB
    final static String CACHE_DATA_CLEANUP_DURING_INITIALIZATION_EXCEPTION = "Failed to delete ehcache disk cache under "
        + "path: % during initialization. Please clean this up manually and restart the process";

    /**
     * Used in computeIfAbsent to synchronize loading of a given key. This is needed as ehcache doesn't provide a
     * computeIfAbsent method.
     */
    Map<ICacheKey<K>, CompletableFuture<Tuple<ICacheKey<K>, V>>> completableFutureMap = new ConcurrentHashMap<>();

    @SuppressForbidden(reason = "Ehcache uses File.io")
    private EhcacheDiskCache(Builder<K, V> builder) {
        this.keyType = Objects.requireNonNull(builder.keyType, "Key type shouldn't be null");
        this.valueType = Objects.requireNonNull(builder.valueType, "Value type shouldn't be null");
        this.expireAfterAccess = Objects.requireNonNull(builder.getExpireAfterAcess(), "ExpireAfterAccess value shouldn't " + "be null");
        this.maxWeightInBytes = builder.getMaxWeightInBytes();
        if (this.maxWeightInBytes <= MINIMUM_MAX_SIZE_IN_BYTES) {
            throw new IllegalArgumentException("Ehcache Disk tier cache size should be greater than " + MINIMUM_MAX_SIZE_IN_BYTES);
        }
        this.cacheType = Objects.requireNonNull(builder.cacheType, "Cache type shouldn't be null");
        if (builder.diskCacheAlias == null || builder.diskCacheAlias.isBlank()) {
            this.diskCacheAlias = "ehcacheDiskCache#" + this.cacheType;
        } else {
            this.diskCacheAlias = builder.diskCacheAlias;
        }
        this.storagePath = builder.storagePath;
        if (this.storagePath == null || this.storagePath.isBlank()) {
            throw new IllegalArgumentException("Storage path shouldn't be null or empty");
        }
        // Delete all the previous disk cache related files/data. We don't persist data between process restart for
        // now which is why need to do this. Clean up in case there was a non graceful restart and we had older disk
        // cache data still lying around.
        Path ehcacheDirectory = Paths.get(this.storagePath);
        if (Files.exists(ehcacheDirectory)) {
            try {
                logger.info("Found older disk cache data lying around during initialization under path: {}", this.storagePath);
                IOUtils.rm(ehcacheDirectory);
            } catch (IOException e) {
                throw new OpenSearchException(String.format(CACHE_DATA_CLEANUP_DURING_INITIALIZATION_EXCEPTION, this.storagePath));
            }
        }
        if (builder.threadPoolAlias == null || builder.threadPoolAlias.isBlank()) {
            this.threadPoolAlias = THREAD_POOL_ALIAS_PREFIX + "DiskWrite#" + UNIQUE_ID;
        } else {
            this.threadPoolAlias = builder.threadPoolAlias;
        }
        this.settings = Objects.requireNonNull(builder.getSettings(), "Settings objects shouldn't be null");
        this.keySerializer = Objects.requireNonNull(builder.keySerializer, "Key serializer shouldn't be null");
        this.valueSerializer = Objects.requireNonNull(builder.valueSerializer, "Value serializer shouldn't be null");
        this.cacheManager = buildCacheManager();
        Objects.requireNonNull(builder.getRemovalListener(), "Removal listener can't be null");
        this.removalListener = builder.getRemovalListener();
        Objects.requireNonNull(builder.getWeigher(), "Weigher can't be null");
        this.ehCacheEventListener = new EhCacheEventListener(builder.getRemovalListener(), builder.getWeigher());
        this.cache = buildCache(Duration.ofMillis(expireAfterAccess.getMillis()), builder);
        List<String> dimensionNames = Objects.requireNonNull(builder.dimensionNames, "Dimension names can't be null");
        if (builder.getStatsTrackingEnabled()) {
            // If this cache is being used, FeatureFlags.PLUGGABLE_CACHE is already on, so we can always use the DefaultCacheStatsHolder
            // unless statsTrackingEnabled is explicitly set to false in CacheConfig.
            this.cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME);
        } else {
            this.cacheStatsHolder = NoopCacheStatsHolder.getInstance();
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private Cache<ICacheKey, ByteArrayWrapper> buildCache(Duration expireAfterAccess, Builder<K, V> builder) {
        // Creating the cache requires permissions specified in plugin-security.policy
        return AccessController.doPrivileged((PrivilegedAction<Cache<ICacheKey, ByteArrayWrapper>>) () -> {
            try {
                return this.cacheManager.createCache(
                    this.diskCacheAlias,
                    CacheConfigurationBuilder.newCacheConfigurationBuilder(
                        ICacheKey.class,
                        ByteArrayWrapper.class,
                        ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B)
                    ).withExpiry(new ExpiryPolicy<>() {
                        @Override
                        public Duration getExpiryForCreation(ICacheKey key, ByteArrayWrapper value) {
                            return INFINITE;
                        }

                        @Override
                        public Duration getExpiryForAccess(ICacheKey key, Supplier<? extends ByteArrayWrapper> value) {
                            return expireAfterAccess;
                        }

                        @Override
                        public Duration getExpiryForUpdate(
                            ICacheKey key,
                            Supplier<? extends ByteArrayWrapper> oldValue,
                            ByteArrayWrapper newValue
                        ) {
                            return INFINITE;
                        }
                    })
                        .withService(getListenerConfiguration(builder))
                        .withService(
                            new OffHeapDiskStoreConfiguration(
                                this.threadPoolAlias,
                                (Integer) EhcacheDiskCacheSettings.getSettingListForCacheType(cacheType)
                                    .get(DISK_WRITE_CONCURRENCY_KEY)
                                    .get(settings),
                                (Integer) EhcacheDiskCacheSettings.getSettingListForCacheType(cacheType).get(DISK_SEGMENT_KEY).get(settings)
                            )
                        )
                        .withKeySerializer(new KeySerializerWrapper(keySerializer))
                        .withValueSerializer(new ByteArrayWrapperSerializer())
                // We pass ByteArrayWrapperSerializer as ehcache's value serializer. If V is an interface, and we pass its
                // serializer directly to ehcache, ehcache requires the classes match exactly before/after serialization.
                // This is not always feasible or necessary, like for BytesReference. So, we handle the value serialization
                // before V hits ehcache.
                );
            } catch (IllegalArgumentException ex) {
                logger.error("Ehcache disk cache initialization failed due to illegal argument: {}", ex.getMessage());
                throw ex;
            } catch (IllegalStateException ex) {
                logger.error("Ehcache disk cache initialization failed: {}", ex.getMessage());
                throw ex;
            }
        });
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
    Map<ICacheKey<K>, CompletableFuture<Tuple<ICacheKey<K>, V>>> getCompletableFutureMap() {
        return completableFutureMap;
    }

    @SuppressForbidden(reason = "Ehcache uses File.io")
    private PersistentCacheManager buildCacheManager() {
        // In case we use multiple ehCaches, we can define this cache manager at a global level.
        // Creating the cache manager also requires permissions specified in plugin-security.policy
        return AccessController.doPrivileged((PrivilegedAction<PersistentCacheManager>) () -> {
            return CacheManagerBuilder.newCacheManagerBuilder()
                .with(CacheManagerBuilder.persistence(new File(storagePath)))

                .using(
                    PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder()
                        .defaultPool(THREAD_POOL_ALIAS_PREFIX + "Default#" + UNIQUE_ID, 1, 3) // Default pool used for other tasks
                        // like event listeners
                        .pool(
                            this.threadPoolAlias,
                            (Integer) EhcacheDiskCacheSettings.getSettingListForCacheType(cacheType)
                                .get(DISK_WRITE_MIN_THREADS_KEY)
                                .get(settings),
                            (Integer) EhcacheDiskCacheSettings.getSettingListForCacheType(cacheType)
                                .get(DISK_WRITE_MAXIMUM_THREADS_KEY)
                                .get(settings)
                        )
                        .build()
                )
                .build(true);
        });
    }

    @Override
    public V get(ICacheKey<K> key) {
        if (key == null) {
            throw new IllegalArgumentException("Key passed to ehcache disk cache was null.");
        }
        V value;
        try {
            value = deserializeValue(cache.get(key));
        } catch (CacheLoadingException ex) {
            throw new OpenSearchException("Exception occurred while trying to fetch item from ehcache disk cache");
        }
        if (value != null) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
        }
        return value;
    }

    /**
     * Puts the item into cache.
     * @param key Type of key.
     * @param value Type of value.
     */
    @Override
    public void put(ICacheKey<K> key, V value) {
        try {
            cache.put(key, serializeValue(value));
        } catch (CacheWritingException ex) {
            throw new OpenSearchException("Exception occurred while put item to ehcache disk cache");
        }
    }

    /**
     * Computes the value using loader in case key is not present, otherwise fetches it.
     * @param key Type of key
     * @param loader loader to load the value in case key is missing
     * @return value
     * @throws Exception when either internal get or put calls fail.
     */
    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        // Ehcache doesn't provide any computeIfAbsent function. Exposes putIfAbsent but that works differently and is
        // not performant in case there are multiple concurrent request for same key. Below is our own custom
        // implementation of computeIfAbsent on top of ehcache. Inspired by OpenSearch Cache implementation.
        V value = deserializeValue(cache.get(key));
        if (value == null) {
            value = compute(key, loader);
        }
        if (!loader.isLoaded()) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
        }
        return value;
    }

    private V compute(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        // A future that returns a pair of key/value.
        CompletableFuture<Tuple<ICacheKey<K>, V>> completableFuture = new CompletableFuture<>();
        // Only one of the threads will succeed putting a future into map for the same key.
        // Rest will fetch existing future.
        CompletableFuture<Tuple<ICacheKey<K>, V>> future = completableFutureMap.putIfAbsent(key, completableFuture);
        // Handler to handle results post processing. Takes a tuple<key, value> or exception as an input and returns
        // the value. Also before returning value, puts the value in cache.
        BiFunction<Tuple<ICacheKey<K>, V>, Throwable, V> handler = (pair, ex) -> {
            V value = null;
            if (pair != null) {
                cache.put(pair.v1(), serializeValue(pair.v2()));
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

    /**
     * Invalidate the item.
     * @param key key to be invalidated.
     */
    @Override
    public void invalidate(ICacheKey<K> key) {
        try {
            if (key.getDropStatsForDimensions()) {
                cacheStatsHolder.removeDimensions(key.dimensions);
            }
            if (key.key != null) {
                cache.remove(key);
            }
        } catch (CacheWritingException ex) {
            // Handle
            throw new RuntimeException(ex);
        }

    }

    @Override
    public void invalidateAll() {
        cache.clear();
        cacheStatsHolder.reset();
    }

    /**
     * Provides a way to iterate over disk cache keys.
     * @return Iterable
     */
    @Override
    public Iterable<ICacheKey<K>> keys() {
        return () -> new EhCacheKeyIterator<>(cache.iterator());
    }

    /**
     * Gives the current count of keys in disk cache.
     * If enableStatsTracking is set to false in the builder, always returns 0.
     * @return current count of keys
     */
    @Override
    public long count() {
        return cacheStatsHolder.count();
    }

    @Override
    public void refresh() {
        // TODO: ehcache doesn't provide a way to refresh a cache.
    }

    @Override
    @SuppressForbidden(reason = "Ehcache uses File.io")
    public void close() {
        try {
            cacheManager.removeCache(this.diskCacheAlias);
            cacheManager.close();
            cacheManager.destroyCache(this.diskCacheAlias);
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Exception occurred while trying to close/remove ehcache"), e);
        } finally {
            // Delete all the disk cache related files/data in case it is present
            Path ehcacheDirectory = Paths.get(this.storagePath);
            if (Files.exists(ehcacheDirectory)) {
                try {
                    IOUtils.rm(ehcacheDirectory);
                } catch (IOException e) {
                    logger.error(
                        () -> new ParameterizedMessage("Failed to delete ehcache disk cache data under path: {}", this.storagePath)
                    );
                }
            }
        }
    }

    /**
     * Relevant stats for this cache, aggregated by levels.
     * @param levels The levels to aggregate by.
     * @return ImmutableCacheStatsHolder
     */
    @Override
    public ImmutableCacheStatsHolder stats(String[] levels) {
        return cacheStatsHolder.getImmutableCacheStatsHolder(levels);
    }

    /**
     * This iterator wraps ehCache iterator and only iterates over its keys.
     * @param <K> Type of key
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    class EhCacheKeyIterator<K> implements Iterator<ICacheKey<K>> {

        Iterator<Cache.Entry<ICacheKey, ByteArrayWrapper>> iterator;

        EhCacheKeyIterator(Iterator<Cache.Entry<ICacheKey, ByteArrayWrapper>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public ICacheKey<K> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next().getKey();
        }

        @Override
        public void remove() {
            iterator.remove(); // Calls underlying ehcache iterator.remove()
        }
    }

    /**
     * Wrapper over Ehcache original listener to listen to desired events and notify desired subscribers.
     */
    class EhCacheEventListener implements CacheEventListener<ICacheKey<K>, ByteArrayWrapper> {
        private final RemovalListener<ICacheKey<K>, V> removalListener;
        private ToLongBiFunction<ICacheKey<K>, V> weigher;

        EhCacheEventListener(RemovalListener<ICacheKey<K>, V> removalListener, ToLongBiFunction<ICacheKey<K>, V> weigher) {
            this.removalListener = removalListener;
            this.weigher = weigher;
        }

        private long getOldValuePairSize(CacheEvent<? extends ICacheKey<K>, ? extends ByteArrayWrapper> event) {
            return weigher.applyAsLong(event.getKey(), deserializeValue(event.getOldValue()));
        }

        private long getNewValuePairSize(CacheEvent<? extends ICacheKey<K>, ? extends ByteArrayWrapper> event) {
            return weigher.applyAsLong(event.getKey(), deserializeValue(event.getNewValue()));
        }

        @Override
        public void onEvent(CacheEvent<? extends ICacheKey<K>, ? extends ByteArrayWrapper> event) {
            switch (event.getType()) {
                case CREATED:
                    cacheStatsHolder.incrementItems(event.getKey().dimensions);
                    cacheStatsHolder.incrementSizeInBytes(event.getKey().dimensions, getNewValuePairSize(event));
                    assert event.getOldValue() == null;
                    break;
                case EVICTED:
                    this.removalListener.onRemoval(
                        new RemovalNotification<>(event.getKey(), deserializeValue(event.getOldValue()), RemovalReason.EVICTED)
                    );
                    cacheStatsHolder.decrementItems(event.getKey().dimensions);
                    cacheStatsHolder.decrementSizeInBytes(event.getKey().dimensions, getOldValuePairSize(event));
                    cacheStatsHolder.incrementEvictions(event.getKey().dimensions);
                    assert event.getNewValue() == null;
                    break;
                case REMOVED:
                    this.removalListener.onRemoval(
                        new RemovalNotification<>(event.getKey(), deserializeValue(event.getOldValue()), RemovalReason.EXPLICIT)
                    );
                    cacheStatsHolder.decrementItems(event.getKey().dimensions);
                    cacheStatsHolder.decrementSizeInBytes(event.getKey().dimensions, getOldValuePairSize(event));
                    assert event.getNewValue() == null;
                    break;
                case EXPIRED:
                    this.removalListener.onRemoval(
                        new RemovalNotification<>(event.getKey(), deserializeValue(event.getOldValue()), RemovalReason.INVALIDATED)
                    );
                    cacheStatsHolder.decrementItems(event.getKey().dimensions);
                    cacheStatsHolder.decrementSizeInBytes(event.getKey().dimensions, getOldValuePairSize(event));
                    assert event.getNewValue() == null;
                    break;
                case UPDATED:
                    long newSize = getNewValuePairSize(event);
                    long oldSize = getOldValuePairSize(event);
                    cacheStatsHolder.incrementSizeInBytes(event.getKey().dimensions, newSize - oldSize);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Wrapper over ICacheKeySerializer which is compatible with ehcache's serializer requirements.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private class KeySerializerWrapper implements org.ehcache.spi.serialization.Serializer<ICacheKey> {
        private ICacheKeySerializer<K> serializer;

        public KeySerializerWrapper(Serializer<K, byte[]> internalKeySerializer) {
            this.serializer = new ICacheKeySerializer<>(internalKeySerializer);
        }

        // This constructor must be present, but does not have to work as we are not actually persisting the disk
        // cache after a restart.
        // See https://www.ehcache.org/documentation/3.0/serializers-copiers.html#persistent-vs-transient-caches
        public KeySerializerWrapper(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {}

        @Override
        public ByteBuffer serialize(ICacheKey object) throws SerializerException {
            return ByteBuffer.wrap(serializer.serialize(object));
        }

        @Override
        public ICacheKey<K> read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            byte[] arr = new byte[binary.remaining()];
            binary.get(arr);
            return serializer.deserialize(arr);
        }

        @Override
        public boolean equals(ICacheKey object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            byte[] arr = new byte[binary.remaining()];
            binary.get(arr);
            return serializer.equals(object, arr);
        }
    }

    /**
     * Wrapper allowing Ehcache to serialize ByteArrayWrapper.
     */
    private static class ByteArrayWrapperSerializer implements org.ehcache.spi.serialization.Serializer<ByteArrayWrapper> {
        public ByteArrayWrapperSerializer() {}

        // This constructor must be present, but does not have to work as we are not actually persisting the disk
        // cache after a restart.
        // See https://www.ehcache.org/documentation/3.0/serializers-copiers.html#persistent-vs-transient-caches
        public ByteArrayWrapperSerializer(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {}

        @Override
        public ByteBuffer serialize(ByteArrayWrapper object) throws SerializerException {
            return ByteBuffer.wrap(object.value);
        }

        @Override
        public ByteArrayWrapper read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            byte[] arr = new byte[binary.remaining()];
            binary.get(arr);
            return new ByteArrayWrapper(arr);
        }

        @Override
        public boolean equals(ByteArrayWrapper object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            byte[] arr = new byte[binary.remaining()];
            binary.get(arr);
            return Arrays.equals(arr, object.value);
        }
    }

    /**
     * Transform a value from V to ByteArrayWrapper, which can be passed to ehcache.
     * @param value the value
     * @return the serialized value
     */
    private ByteArrayWrapper serializeValue(V value) {
        return new ByteArrayWrapper(valueSerializer.serialize(value));
    }

    /**
     * Transform a ByteArrayWrapper, which comes from ehcache, back to V.
     * @param binary the serialized value
     * @return the deserialized value
     */
    private V deserializeValue(ByteArrayWrapper binary) {
        if (binary == null) {
            return null;
        }
        return valueSerializer.deserialize(binary.value);
    }

    /**
     * Factory to create an ehcache disk cache.
     */
    public static class EhcacheDiskCacheFactory implements ICache.Factory {

        /**
         * Ehcache disk cache name.
         */
        public static final String EHCACHE_DISK_CACHE_NAME = "ehcache_disk";

        /**
         * Default constructor.
         */
        public EhcacheDiskCacheFactory() {}

        @Override
        @SuppressWarnings({ "unchecked" }) // Required to ensure the serializers output byte[]
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            Map<String, Setting<?>> settingList = EhcacheDiskCacheSettings.getSettingListForCacheType(cacheType);
            Settings settings = config.getSettings();

            Serializer<K, byte[]> keySerializer = null;
            try {
                keySerializer = (Serializer<K, byte[]>) config.getKeySerializer();
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("EhcacheDiskCache requires a key serializer of type Serializer<K, byte[]>");
            }

            Serializer<V, byte[]> valueSerializer = null;
            try {
                valueSerializer = (Serializer<V, byte[]>) config.getValueSerializer();
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("EhcacheDiskCache requires a value serializer of type Serializer<V, byte[]>");
            }

            return new Builder<K, V>().setStoragePath((String) settingList.get(DISK_STORAGE_PATH_KEY).get(settings))
                .setDiskCacheAlias((String) settingList.get(DISK_CACHE_ALIAS_KEY).get(settings))
                .setIsEventListenerModeSync((Boolean) settingList.get(DISK_LISTENER_MODE_SYNC_KEY).get(settings))
                .setCacheType(cacheType)
                .setKeyType((config.getKeyType()))
                .setValueType(config.getValueType())
                .setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .setDimensionNames(config.getDimensionNames())
                .setWeigher(config.getWeigher())
                .setRemovalListener(config.getRemovalListener())
                .setExpireAfterAccess((TimeValue) settingList.get(DISK_CACHE_EXPIRE_AFTER_ACCESS_KEY).get(settings))
                .setMaximumWeightInBytes((Long) settingList.get(DISK_MAX_SIZE_IN_BYTES_KEY).get(settings))
                .setSettings(settings)
                .build();
        }

        @Override
        public String getCacheName() {
            return EHCACHE_DISK_CACHE_NAME;
        }
    }

    /**
     * Builder object to build Ehcache disk tier.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> extends ICacheBuilder<K, V> {

        private CacheType cacheType;
        private String storagePath;

        private String threadPoolAlias;

        private String diskCacheAlias;

        // Provides capability to make ehCache event listener to run in sync mode. Used for testing too.
        private boolean isEventListenerModeSync;

        private Class<K> keyType;

        private Class<V> valueType;
        private List<String> dimensionNames;
        private Serializer<K, byte[]> keySerializer;
        private Serializer<V, byte[]> valueSerializer;

        /**
         * Default constructor. Added to fix javadocs.
         */
        public Builder() {}

        /**
         * Sets the desired cache type.
         * @param cacheType cache type
         * @return builder
         */
        public Builder<K, V> setCacheType(CacheType cacheType) {
            this.cacheType = cacheType;
            return this;
        }

        /**
         * Sets the key type of value.
         * @param keyType type of key
         * @return builder
         */
        public Builder<K, V> setKeyType(Class<K> keyType) {
            this.keyType = keyType;
            return this;
        }

        /**
         * Sets the class type of value.
         * @param valueType type of value
         * @return builder
         */
        public Builder<K, V> setValueType(Class<V> valueType) {
            this.valueType = valueType;
            return this;
        }

        /**
         * Desired storage path for disk cache.
         * @param storagePath path for disk cache
         * @return builder
         */
        public Builder<K, V> setStoragePath(String storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        /**
         * Thread pool alias for the cache.
         * @param threadPoolAlias alias
         * @return builder
         */
        public Builder<K, V> setThreadPoolAlias(String threadPoolAlias) {
            this.threadPoolAlias = threadPoolAlias;
            return this;
        }

        /**
         * Cache alias
         * @param diskCacheAlias disk cache alias
         * @return builder
         */
        public Builder<K, V> setDiskCacheAlias(String diskCacheAlias) {
            this.diskCacheAlias = diskCacheAlias;
            return this;
        }

        /**
         * Determines whether event listener is triggered async/sync.
         * @param isEventListenerModeSync mode sync
         * @return builder
         */
        public Builder<K, V> setIsEventListenerModeSync(boolean isEventListenerModeSync) {
            this.isEventListenerModeSync = isEventListenerModeSync;
            return this;
        }

        /**
         * Sets the allowed dimension names for keys that will enter this cache.
         * @param dimensionNames A list of dimension names this cache will accept
         * @return builder
         */
        public Builder<K, V> setDimensionNames(List<String> dimensionNames) {
            this.dimensionNames = dimensionNames;
            return this;
        }

        /**
         * Sets the key serializer for this cache.
         * @param keySerializer the key serializer
         * @return builder
         */
        public Builder<K, V> setKeySerializer(Serializer<K, byte[]> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        /**
         * Sets the value serializer for this cache.
         * @param valueSerializer the value serializer
         * @return builder
         */
        public Builder<K, V> setValueSerializer(Serializer<V, byte[]> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        @Override
        public EhcacheDiskCache<K, V> build() {
            return new EhcacheDiskCache<>(this);
        }
    }

    /**
     * A wrapper over byte[], with equals() that works using Arrays.equals().
     * Necessary due to a limitation in how Ehcache compares byte[].
     */
    static class ByteArrayWrapper {
        private final byte[] value;

        public ByteArrayWrapper(byte[] value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != ByteArrayWrapper.class) {
                return false;
            }
            ByteArrayWrapper other = (ByteArrayWrapper) o;
            return Arrays.equals(this.value, other.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }
}
