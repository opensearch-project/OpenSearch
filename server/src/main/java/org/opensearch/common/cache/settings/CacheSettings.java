/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.settings;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.settings.Setting;

/**
 * Settings related to cache.
 */
@ExperimentalApi
public class CacheSettings {

    /**
     * Stores a disk cache store name for cache types within OpenSearch.
     * Setting pattern: {cache_type}.disk.store.name. Example: indices.request.cache.disk.store.name
     */
    public static final Setting.AffixSetting<String> CACHE_TYPE_DISK_STORE_NAME = Setting.suffixKeySetting(
        "disk.store.name",
        (key) -> Setting.simpleString(key, "", Setting.Property.NodeScope)
    );

    /**
     * Stores an onHeap cache store name for cache types within OpenSearch.
     * Setting pattern: {cache_type}.onheap.store.name.
     */
    public static final Setting.AffixSetting<String> CACHE_TYPE_ONHEAP_STORE_NAME = Setting.suffixKeySetting(
        "onheap.store.name",
        (key) -> Setting.simpleString(key, "", Setting.Property.NodeScope)
    );

    public static Setting<String> getConcreteSettingForCacheType(CacheType cacheType, CacheStoreType cacheStoreType) {
        switch (cacheStoreType) {
            case DISK:
                return CACHE_TYPE_DISK_STORE_NAME.getConcreteSettingForNamespace(cacheType.getSettingPrefix());
            case ON_HEAP:
                return CACHE_TYPE_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(cacheType.getSettingPrefix());
            default:
                throw new IllegalArgumentException("Invalid cache store type: " + cacheStoreType);
        }
    }
}
