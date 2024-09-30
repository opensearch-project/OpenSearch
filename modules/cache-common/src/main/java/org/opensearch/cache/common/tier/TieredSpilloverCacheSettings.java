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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.common.cache.settings.CacheSettings.VALID_SEGMENT_NUMBER_LIST;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

/**
 * Settings related to TieredSpilloverCache.
 */
public class TieredSpilloverCacheSettings {

    /**
     * Exception message for invalid segment number.
     */
    public static final String INVALID_SEGMENT_NUMBER_EXCEPTION_MESSAGE = "Tiered cache segment number should be "
        + "power of two up-to 256";
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
        (key) -> Setting.intSetting(key, 16, 1, k -> {
            if (!VALID_SEGMENT_NUMBER_LIST.contains(k)) {
                throw new IllegalArgumentException(INVALID_SEGMENT_NUMBER_EXCEPTION_MESSAGE);
            }
        }, NodeScope)
    );

    /**
     * Setting defining the minimum took time for a query to be allowed into the disk cache.
     */
    private static final Setting.AffixSetting<TimeValue> TIERED_SPILLOVER_DISK_TOOK_TIME_THRESHOLD = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".disk.store.policies.took_time.threshold",
        (key) -> Setting.timeSetting(
            key,
            new TimeValue(10, TimeUnit.MILLISECONDS), // Default value for this setting
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
     * Stores disk cache enabled settings for various cache types as these are dynamic so that can be registered and
     * retrieved accordingly.
     */
    public static final Map<CacheType, Setting<Boolean>> DISK_CACHE_ENABLED_SETTING_MAP;

    /**
     * Fetches concrete took time policy and disk cache settings.
     */
    static {
        Map<CacheType, Setting<TimeValue>> concreteTookTimePolicySettingMap = new HashMap<>();
        Map<CacheType, Setting<Boolean>> diskCacheSettingMap = new HashMap<>();
        for (CacheType cacheType : CacheType.values()) {
            concreteTookTimePolicySettingMap.put(
                cacheType,
                TIERED_SPILLOVER_DISK_TOOK_TIME_THRESHOLD.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
            );
            diskCacheSettingMap.put(
                cacheType,
                TIERED_SPILLOVER_DISK_CACHE_SETTING.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
            );
        }
        TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP = concreteTookTimePolicySettingMap;
        DISK_CACHE_ENABLED_SETTING_MAP = diskCacheSettingMap;
    }

    /**
     * Default constructor
     */
    TieredSpilloverCacheSettings() {}
}
