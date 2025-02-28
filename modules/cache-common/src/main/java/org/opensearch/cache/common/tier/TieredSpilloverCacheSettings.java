/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.common.cache.settings.CacheSettings.INVALID_SEGMENT_COUNT_EXCEPTION_MESSAGE;
import static org.opensearch.common.cache.settings.CacheSettings.VALID_SEGMENT_COUNT_VALUES;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

/**
 * Settings related to TieredSpilloverCache.
 */
public class TieredSpilloverCacheSettings {

    /**
     * Default cache size in bytes ie 1gb.
     */
    public static final long DEFAULT_DISK_CACHE_SIZE_IN_BYTES = 1073741824L;

    /**
     * Minimum disk cache size ie 10mb. May not make such sense to keep a value smaller than this.
     */
    public static final long MIN_DISK_CACHE_SIZE_IN_BYTES = 10485760L;

    /**
     * The default took time threshold for a value to enter the heap tier of the cache, and therefore to enter the cache at all.
     */
    public static final TimeValue DEFAULT_TOOK_TIME_THRESHOLD = TimeValue.ZERO;

    /**
     * The default took time threshold for a value to enter the disk tier of the cache.
     */
    public static final TimeValue DEFAULT_TOOK_TIME_DISK_THRESHOLD = new TimeValue(10, TimeUnit.MILLISECONDS);

    /**
     * Setting which defines the onHeap cache store to be used in TieredSpilloverCache.
     *
     * Pattern: {cache_type}.tiered_spillover.onheap.store.name
     * Example: indices.request.cache.tiered_spillover.onheap.store.name
     */
    public static final Setting.AffixSetting<String> TIERED_SPILLOVER_ONHEAP_STORE_NAME = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".onheap.store.name",
        (key) -> Setting.simpleString(key, "", NodeScope)
    );

    /**
     * Setting which defines the disk cache store to be used in TieredSpilloverCache.
     */
    public static final Setting.AffixSetting<String> TIERED_SPILLOVER_DISK_STORE_NAME = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".disk.store.name",
        (key) -> Setting.simpleString(key, "", NodeScope)
    );

    /**
     * Setting to disable/enable disk cache dynamically.
     */
    public static final Setting.AffixSetting<Boolean> TIERED_SPILLOVER_DISK_CACHE_SETTING = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".disk.store.enabled",
        (key) -> Setting.boolSetting(key, true, NodeScope, Setting.Property.Dynamic)
    );

    /**
     * Setting defining the number of segments within Tiered cache
     */
    public static final Setting.AffixSetting<Integer> TIERED_SPILLOVER_SEGMENTS = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".segments",
        (key) -> Setting.intSetting(key, defaultSegments(), 1, k -> {
            if (!VALID_SEGMENT_COUNT_VALUES.contains(k)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        INVALID_SEGMENT_COUNT_EXCEPTION_MESSAGE,
                        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
                    )
                );
            }
        }, NodeScope)
    );

    /**
     * Setting which defines the onHeap cache size to be used within tiered cache.
     * This setting overrides size settings from the heap tier implementation.
     * For example, if OpenSearchOnHeapCache is the heap tier in the request cache, and
     * indices.requests.cache.opensearch_onheap.size is set, that value will be ignored in favor of this setting.
     *
     * Pattern: {cache_type}.tiered_spillover.onheap.store.size
     * Example: indices.request.cache.tiered_spillover.onheap.store.size
     */
    public static final Setting.AffixSetting<ByteSizeValue> TIERED_SPILLOVER_ONHEAP_STORE_SIZE = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".onheap.store.size",
        (key) -> Setting.memorySizeSetting(key, "1%", NodeScope)
    );

    /**
     * Setting which defines the disk cache size to be used within tiered cache.
     * This setting overrides the size setting from the disk tier implementation.
     * For example, if EhcacheDiskCache is the disk tier in the request cache, and
     * indices.requests.cache.ehcache_disk.max_size_in_bytes is set, that value will be ignored in favor of this setting.
     */
    public static final Setting.AffixSetting<Long> TIERED_SPILLOVER_DISK_STORE_SIZE = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".disk.store.size",
        (key) -> Setting.longSetting(key, DEFAULT_DISK_CACHE_SIZE_IN_BYTES, MIN_DISK_CACHE_SIZE_IN_BYTES, NodeScope)
    );

    /**
     * Setting defining the minimum took time for a query to be allowed in the cache.
     */
    private static final Setting.AffixSetting<TimeValue> TIERED_SPILLOVER_TOOK_TIME_THRESHOLD = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".policies.took_time.threshold",
        (key) -> Setting.timeSetting(
            key,
            DEFAULT_TOOK_TIME_THRESHOLD,
            TimeValue.ZERO, // Minimum value for this setting
            NodeScope,
            Setting.Property.Dynamic
        )
    );

    /**
     * Setting defining the minimum took time for a query to be allowed in the disk tier of the cache.
     */
    private static final Setting.AffixSetting<TimeValue> TIERED_SPILLOVER_DISK_TOOK_TIME_THRESHOLD = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".disk.store.policies.took_time.threshold",
        (key) -> Setting.timeSetting(
            key,
            DEFAULT_TOOK_TIME_DISK_THRESHOLD,
            TimeValue.ZERO, // Minimum value for this setting
            NodeScope,
            Setting.Property.Dynamic
        )
    );

    /**
     * Stores took time policy settings for various cache types as these are dynamic so that can be registered and
     * retrieved accordingly.
     */
    public static final Map<CacheType, Setting<TimeValue>> TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP;

    /**
     * Stores took time policy settings for the disk tiers of various cache types as these are dynamic so that can be registered and
     * retrieved accordingly.
     */
    public static final Map<CacheType, Setting<TimeValue>> TOOK_TIME_DISK_TIER_POLICY_CONCRETE_SETTINGS_MAP;

    /**
     * Stores disk cache enabled settings for various cache types as these are dynamic so that can be registered and
     * retrieved accordingly.
     */
    public static final Map<CacheType, Setting<Boolean>> DISK_CACHE_ENABLED_SETTING_MAP;

    /**
     * Fetches concrete took time policy and disk cache settings.
     */
    static {
        Map<CacheType, Setting<TimeValue>> concreteTookTimePolicySettingMap = new HashMap<>();
        Map<CacheType, Setting<TimeValue>> concreteDiskTookTimePolicySettingMap = new HashMap<>();
        Map<CacheType, Setting<Boolean>> diskCacheSettingMap = new HashMap<>();
        for (CacheType cacheType : CacheType.values()) {
            concreteTookTimePolicySettingMap.put(
                cacheType,
                TIERED_SPILLOVER_TOOK_TIME_THRESHOLD.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
            );
            concreteDiskTookTimePolicySettingMap.put(
                cacheType,
                TIERED_SPILLOVER_DISK_TOOK_TIME_THRESHOLD.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
            );
            diskCacheSettingMap.put(
                cacheType,
                TIERED_SPILLOVER_DISK_CACHE_SETTING.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
            );
        }
        TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP = concreteTookTimePolicySettingMap;
        TOOK_TIME_DISK_TIER_POLICY_CONCRETE_SETTINGS_MAP = concreteDiskTookTimePolicySettingMap;
        DISK_CACHE_ENABLED_SETTING_MAP = diskCacheSettingMap;
    }

    /**
     * Returns the default segment count to be used within TieredCache.
     * @return default segment count
     */
    public static int defaultSegments() {
        // For now, we use number of search threads as the default segment count. If needed each cache type can
        // configure its own segmentCount via setting in the future.
        int defaultSegmentCount = ThreadPool.searchThreadPoolSize(Runtime.getRuntime().availableProcessors());
        // Now round it off to the next power of 2 as we don't support any other values.
        for (int segmentValue : VALID_SEGMENT_COUNT_VALUES) {
            if (defaultSegmentCount <= segmentValue) {
                return segmentValue;
            }
        }
        return VALID_SEGMENT_COUNT_VALUES.get(VALID_SEGMENT_COUNT_VALUES.size() - 1);
    }

    /**
     * Default constructor
     */
    TieredSpilloverCacheSettings() {}
}
