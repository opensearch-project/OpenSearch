/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import org.opensearch.cache.store.disk.EhcacheDiskCache;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.settings.Setting.Property.NodeScope;

/**
 * Settings related to ehcache disk cache.
 */
public class EhcacheDiskCacheSettings {

    /**
     * Default cache size in bytes ie 1gb.
     */
    public static final long DEFAULT_CACHE_SIZE_IN_BYTES = 1073741824L;

    /**
     * Ehcache disk write minimum threads for its pool
     *
     * Setting pattern: {cache_type}.ehcache_disk.min_threads
     */

    public static final Setting.AffixSetting<Integer> DISK_WRITE_MINIMUM_THREADS_SETTING = Setting.suffixKeySetting(
        EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME + ".min_threads",
        (key) -> Setting.intSetting(key, 2, 1, 5, NodeScope)
    );

    /**
     *  Ehcache disk write maximum threads for its pool
     *
     *  Setting pattern: {cache_type}.ehcache_disk.max_threads
     */
    public static final Setting.AffixSetting<Integer> DISK_WRITE_MAXIMUM_THREADS_SETTING = Setting.suffixKeySetting(
        EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME + ".max_threads",
        (key) -> Setting.intSetting(key, 2, 1, 20, NodeScope)
    );

    /**
     *  Not be to confused with number of disk segments, this is different. Defines
     *  distinct write queues created for disk store where a group of segments share a write queue. This is
     *  implemented with ehcache using a partitioned thread pool exectutor By default all segments share a single write
     *  queue ie write concurrency is 1. Check OffHeapDiskStoreConfiguration and DiskWriteThreadPool.
     *
     *  Default is 1 within ehcache.
     *
     *
     */
    public static final Setting.AffixSetting<Integer> DISK_WRITE_CONCURRENCY_SETTING = Setting.suffixKeySetting(
        EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME + ".concurrency",
        (key) -> Setting.intSetting(key, 1, 1, 3, NodeScope)
    );

    /**
     * Defines how many segments the disk cache is separated into. Higher number achieves greater concurrency but
     * will hold that many file pointers. Default is 16.
     *
     * Default value is 16 within Ehcache.
     */
    public static final Setting.AffixSetting<Integer> DISK_SEGMENTS_SETTING = Setting.suffixKeySetting(
        EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME + ".segments",
        (key) -> Setting.intSetting(key, 16, 1, 32, NodeScope)
    );

    /**
     * Storage path for disk cache.
     */
    public static final Setting.AffixSetting<String> DISK_STORAGE_PATH_SETTING = Setting.suffixKeySetting(
        EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME + ".storage.path",
        (key) -> Setting.simpleString(key, "", NodeScope)
    );

    /**
     * Disk cache alias.
     */
    public static final Setting.AffixSetting<String> DISK_CACHE_ALIAS_SETTING = Setting.suffixKeySetting(
        EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME + ".alias",
        (key) -> Setting.simpleString(key, "", NodeScope)
    );

    /**
     * Disk cache expire after access setting.
     */
    public static final Setting.AffixSetting<TimeValue> DISK_CACHE_EXPIRE_AFTER_ACCESS_SETTING = Setting.suffixKeySetting(
        EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME + ".expire_after_access",
        (key) -> Setting.positiveTimeSetting(key, TimeValue.MAX_VALUE, NodeScope)
    );

    /**
     * Disk cache max size setting.
     */
    public static final Setting.AffixSetting<Long> DISK_CACHE_MAX_SIZE_IN_BYTES_SETTING = Setting.suffixKeySetting(
        EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME + ".max_size_in_bytes",
        (key) -> Setting.longSetting(key, DEFAULT_CACHE_SIZE_IN_BYTES, NodeScope)
    );

    /**
     * Disk cache listener mode setting.
     */
    public static final Setting.AffixSetting<Boolean> DISK_CACHE_LISTENER_MODE_SYNC_SETTING = Setting.suffixKeySetting(
        EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME + ".is_event_listener_sync",
        (key) -> Setting.boolSetting(key, false, NodeScope)
    );

    /**
     * Key for disk segment.
     */
    public static final String DISK_SEGMENT_KEY = "disk_segment";
    /**
     * Key for max size.
     */
    public static final String DISK_MAX_SIZE_IN_BYTES_KEY = "max_size_in_bytes";
    /**
     * Key for expire after access.
     */
    public static final String DISK_CACHE_EXPIRE_AFTER_ACCESS_KEY = "disk_cache_expire_after_access_key";
    /**
     * Key for cache alias.
     */
    public static final String DISK_CACHE_ALIAS_KEY = "disk_cache_alias";
    /**
     * Key for disk segment.
     */
    public static final String DISK_SEGMENTS_KEY = "disk_segments";
    /**
     * Key for disk write concurrency.
     */
    public static final String DISK_WRITE_CONCURRENCY_KEY = "disk_write_concurrency";
    /**
     * Key for max threads.
     */
    public static final String DISK_WRITE_MAXIMUM_THREADS_KEY = "disk_write_max_threads";
    /**
     * Key for min threads.
     */
    public static final String DISK_WRITE_MIN_THREADS_KEY = "disk_write_min_threads";
    /**
     * Key for storage path.
     */
    public static final String DISK_STORAGE_PATH_KEY = "disk_storage_path";
    /**
     * Key for listener mode
     */
    public static final String DISK_LISTENER_MODE_SYNC_KEY = "disk_listener_mode";

    /**
     * Map of key to setting.
     */
    private static final Map<String, Setting.AffixSetting<?>> KEY_SETTING_MAP = Map.of(
        DISK_SEGMENT_KEY,
        DISK_SEGMENTS_SETTING,
        DISK_CACHE_EXPIRE_AFTER_ACCESS_KEY,
        DISK_CACHE_EXPIRE_AFTER_ACCESS_SETTING,
        DISK_CACHE_ALIAS_KEY,
        DISK_CACHE_ALIAS_SETTING,
        DISK_WRITE_CONCURRENCY_KEY,
        DISK_WRITE_CONCURRENCY_SETTING,
        DISK_WRITE_MAXIMUM_THREADS_KEY,
        DISK_WRITE_MAXIMUM_THREADS_SETTING,
        DISK_WRITE_MIN_THREADS_KEY,
        DISK_WRITE_MINIMUM_THREADS_SETTING,
        DISK_STORAGE_PATH_KEY,
        DISK_STORAGE_PATH_SETTING,
        DISK_MAX_SIZE_IN_BYTES_KEY,
        DISK_CACHE_MAX_SIZE_IN_BYTES_SETTING,
        DISK_LISTENER_MODE_SYNC_KEY,
        DISK_CACHE_LISTENER_MODE_SYNC_SETTING
    );

    /**
     * Map to store desired settings for a cache type.
     */
    public static final Map<CacheType, Map<String, Setting<?>>> CACHE_TYPE_MAP = getCacheTypeMap();

    /**
     * Used to form concrete setting for cache types and return desired map
     * @return map of cacheType and associated settings.
     */
    private static final Map<CacheType, Map<String, Setting<?>>> getCacheTypeMap() {
        Map<CacheType, Map<String, Setting<?>>> cacheTypeMap = new HashMap<>();
        for (CacheType cacheType : CacheType.values()) {
            Map<String, Setting<?>> settingMap = new HashMap<>();
            for (Map.Entry<String, Setting.AffixSetting<?>> entry : KEY_SETTING_MAP.entrySet()) {
                settingMap.put(entry.getKey(), entry.getValue().getConcreteSettingForNamespace(cacheType.getSettingPrefix()));
            }
            cacheTypeMap.put(cacheType, settingMap);
        }
        return cacheTypeMap;
    }

    /**
     * Fetches setting list for a combination of cache type and store name.
     * @param cacheType cache type
     * @return settings
     */
    public static final Map<String, Setting<?>> getSettingListForCacheType(CacheType cacheType) {
        Map<String, Setting<?>> cacheTypeSettings = CACHE_TYPE_MAP.get(cacheType);
        if (cacheTypeSettings == null) {
            throw new IllegalArgumentException(
                "No settings exist for cache store name: "
                    + EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME
                    + "associated with "
                    + "cache type: "
                    + cacheType
            );
        }
        return cacheTypeSettings;
    }

    /**
     * Default constructor. Added to fix javadocs.
     */
    public EhcacheDiskCacheSettings() {}
}
