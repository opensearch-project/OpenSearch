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
import org.opensearch.cache.EhcacheDiskCacheSettings;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;

import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_WRITE_MAXIMUM_THREADS_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_WRITE_MIN_THREADS_KEY;
import static org.opensearch.cache.store.disk.EhcacheDiskCache.THREAD_POOL_ALIAS_PREFIX;
import static org.opensearch.cache.store.disk.EhcacheDiskCache.UNIQUE_ID;

/**
 * This is responsible to create a single cache manager for a cache type, and is used to create subsequent caches if
 * needed.
 */
public class EhcacheDiskCacheManager {

    // Defines one cache manager per cache type.
    private static final Map<CacheType, Tuple<PersistentCacheManager, AtomicInteger>> cacheManagerMap = new HashMap<>();
    private static final Logger logger = LogManager.getLogger(EhcacheDiskCacheManager.class);
    /**
     * This lock is used to synchronize the operation where we create/remove cache and increment/decrement the
     * reference counters.
     */
    private static final Lock lock = new ReentrantLock();
    private static final String CACHE_MANAGER_DOES_NOT_EXIST_EXCEPTION_MSG = "Ehcache manager does not exist for " + "cache type: ";

    // For testing
    static Map<CacheType, Tuple<PersistentCacheManager, AtomicInteger>> getCacheManagerMap() {
        return cacheManagerMap;
    }

    /**
     * Private Constructor
     */
    private EhcacheDiskCacheManager() {}

    /**
     * Used to fetch cache manager for a cache type. If it doesn't exist, it creates one.
     * @param cacheType cache type
     * @param storagePath storage path for the cache
     * @param settings settings
     * @param threadPoolAlias alias for disk thread pool
     * @return persistent cache manager
     */
    public static PersistentCacheManager getCacheManager(
        CacheType cacheType,
        String storagePath,
        Settings settings,
        String threadPoolAlias
    ) {
        try {
            lock.lock();
            return cacheManagerMap.computeIfAbsent(
                cacheType,
                type -> new Tuple<>(createCacheManager(cacheType, storagePath, settings, threadPoolAlias), new AtomicInteger(0))
            ).v1();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks whether cache manager exist for a cache type.
     * @param cacheType cache type
     * @return true/false
     */
    public static boolean doesCacheManagerExist(CacheType cacheType) {
        return cacheManagerMap.get(cacheType) != null;
    }

    /**
     * Used to create caches using a cache manager for a specific cache type.
     * @param cacheType cache type
     * @param diskCacheAlias disk cache alias
     * @param cacheConfigurationBuilder cache configuration
     * @return disk cache
     * @param <K> key type
     * @param <V> value type
     */
    @SuppressWarnings({ "removal" })
    public static <K, V> Cache<K, V> createCache(
        CacheType cacheType,
        String diskCacheAlias,
        CacheConfigurationBuilder<K, V> cacheConfigurationBuilder
    ) {
        if (cacheType == null || diskCacheAlias == null || cacheConfigurationBuilder == null) {
            throw new IllegalArgumentException(
                "One of the arguments passed to createCache is "
                    + "null. CacheType: "
                    + cacheType
                    + ", diskCacheAlias: "
                    + diskCacheAlias
                    + ", "
                    + "cacheConfigurationBuilder: "
                    + cacheConfigurationBuilder
            );
        }
        if (cacheManagerMap.get(cacheType) == null) {
            throw new IllegalArgumentException(CACHE_MANAGER_DOES_NOT_EXIST_EXCEPTION_MSG + cacheType);
        }
        // Creating the cache requires permissions specified in plugin-security.policy
        return AccessController.doPrivileged((PrivilegedAction<Cache<K, V>>) () -> {
            try {
                lock.lock();
                // Check again for null cache manager, in case it got removed by another thread in below closeCache()
                // method.
                if (cacheManagerMap.get(cacheType) == null) {
                    logger.warn(CACHE_MANAGER_DOES_NOT_EXIST_EXCEPTION_MSG + cacheType);
                    throw new IllegalStateException(CACHE_MANAGER_DOES_NOT_EXIST_EXCEPTION_MSG + cacheType);
                }
                Cache<K, V> cache = cacheManagerMap.get(cacheType).v1().createCache(diskCacheAlias, cacheConfigurationBuilder
                // We pass ByteArrayWrapperSerializer as ehcache's value serializer. If V is an interface, and we pass its
                // serializer directly to ehcache, ehcache requires the classes match exactly before/after serialization.
                // This is not always feasible or necessary, like for BytesReference. So, we handle the value serialization
                // before V hits ehcache.
                );
                cacheManagerMap.get(cacheType).v2().incrementAndGet();
                return cache;
            } catch (IllegalArgumentException ex) {
                logger.error("Ehcache disk cache initialization failed due to illegal argument: {}", ex.getMessage());
                throw ex;
            } catch (IllegalStateException ex) {
                logger.error("Ehcache disk cache initialization failed: {}", ex.getMessage());
                throw ex;
            } finally {
                lock.unlock();
            }
        });
    }

    /**
     * Used to close cache for a specific cache type and alias.
     * @param cacheType cache type
     * @param diskCacheAlias disk cache alias
     * @param storagePath storage path for cache
     */
    @SuppressForbidden(reason = "Ehcache uses File.io")
    public static void closeCache(CacheType cacheType, String diskCacheAlias, String storagePath) {
        if (cacheManagerMap.get(cacheType) == null) {
            logger.warn(() -> new ParameterizedMessage("Trying to close cache for: {} but cache manager does not " + "exist", cacheType));
            return;
        }
        PersistentCacheManager cacheManager = cacheManagerMap.get(cacheType).v1();
        try {
            lock.lock();
            try {
                cacheManager.removeCache(diskCacheAlias);
            } catch (Exception ex) {
                logger.error(() -> new ParameterizedMessage("Exception occurred while trying to close cache: " + diskCacheAlias), ex);
            }
            // Check again in case a different thread removed it.
            if (cacheManagerMap.get(cacheType) == null) {
                logger.warn(
                    () -> new ParameterizedMessage("Trying to close cache for: {} but cache manager does not " + "exist", cacheType)
                );
                return;
            }
            int referenceCount = cacheManagerMap.get(cacheType).v2().decrementAndGet();
            // All caches have been closed associated with this cache manager, lets close this as well.
            if (referenceCount == 0) {
                try {
                    logger.debug("Closing cache manager for cacheType: " + cacheType);
                    cacheManager.close();
                } catch (Exception e) {
                    logger.error(() -> new ParameterizedMessage("Exception occurred while trying to close ehcache manager"), e);
                }
                // Delete all the disk cache related files/data in case it is present
                Path ehcacheDirectory = Paths.get(storagePath);
                if (Files.exists(ehcacheDirectory)) {
                    try {
                        logger.debug(
                            "Removing disk cache related files for cacheType: " + cacheType + " under " + "directory: " + ehcacheDirectory
                        );
                        IOUtils.rm(ehcacheDirectory);
                    } catch (IOException e) {
                        logger.error(
                            () -> new ParameterizedMessage("Failed to delete ehcache disk cache data under path: {}", storagePath)
                        );
                    }
                }
                cacheManagerMap.remove(cacheType);
            }
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("removal")
    @SuppressForbidden(reason = "Ehcache uses File.io")
    private static PersistentCacheManager createCacheManager(
        CacheType cacheType,
        String storagePath,
        Settings settings,
        String threadPoolAlias
    ) {

        return AccessController.doPrivileged(
            (PrivilegedAction<PersistentCacheManager>) () -> CacheManagerBuilder.newCacheManagerBuilder()
                .with(CacheManagerBuilder.persistence(new File(storagePath)))

                .using(
                    PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder()
                        .defaultPool(THREAD_POOL_ALIAS_PREFIX + "Default#" + UNIQUE_ID, 1, 3) // Default pool used for other tasks
                        // like event listeners
                        .pool(
                            threadPoolAlias,
                            (Integer) EhcacheDiskCacheSettings.getSettingListForCacheType(cacheType)
                                .get(DISK_WRITE_MIN_THREADS_KEY)
                                .get(settings),
                            (Integer) EhcacheDiskCacheSettings.getSettingListForCacheType(cacheType)
                                .get(DISK_WRITE_MAXIMUM_THREADS_KEY)
                                .get(settings)
                        )
                        .build()
                )
                .build(true)
        );
    }
}
