/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.settings;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.settings.Setting.Property.NodeScope;

/**
 * Settings for OpenSearchOnHeap
 */
public class OpenSearchOnHeapCacheSettings {

    /**
     * Setting to define maximum size for the cache as a percentage of heap memory available.
     *
     * Setting pattern: {cache_type}.opensearch_onheap.size
     */
    public static final Setting.AffixSetting<ByteSizeValue> MAXIMUM_SIZE_IN_BYTES = Setting.suffixKeySetting(
        OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME + ".size",
        (key) -> Setting.memorySizeSetting(key, "1%", NodeScope)
    );

    public static final String MAXIMUM_SIZE_IN_BYTES_KEY = "maximum_size_in_bytes";

    private static final Map<String, Setting.AffixSetting<?>> KEY_SETTING_MAP = Map.of(MAXIMUM_SIZE_IN_BYTES_KEY, MAXIMUM_SIZE_IN_BYTES);

    public static final Map<CacheType, Map<String, Setting<?>>> CACHE_TYPE_MAP = getCacheTypeMap();

    private static Map<CacheType, Map<String, Setting<?>>> getCacheTypeMap() {
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

    public static Map<String, Setting<?>> getSettingListForCacheType(CacheType cacheType) {
        Map<String, Setting<?>> cacheTypeSettings = CACHE_TYPE_MAP.get(cacheType);
        if (cacheTypeSettings == null) {
            throw new IllegalArgumentException(
                "No settings exist for cache store name: "
                    + OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
                    + "associated with "
                    + "cache type: "
                    + cacheType
            );
        }
        return cacheTypeSettings;
    }
}
