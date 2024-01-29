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
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.cache.EhcacheSettings.CACHE_TYPE_MAP;

/**
 * Ehcache based cache plugin.
 */
public class EhcacheCachePlugin extends Plugin implements CachePlugin {

    private static final String EHCACHE_CACHE_PLUGIN = "EhcachePlugin";

    /**
     * Default constructor to avoid javadoc related failures.
     */
    public EhcacheCachePlugin() {}

    @Override
    public Map<CacheStoreType, StoreAwareCache.Factory> getCacheStoreTypeMap() {
        return Map.of(CacheStoreType.DISK, new EhcacheDiskCache.EhcacheDiskCacheFactory());
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        for (Map.Entry<CacheType, Map<CacheStoreType, Map<String, Setting<?>>>> entry : CACHE_TYPE_MAP.entrySet()) {
            for (Map.Entry<CacheStoreType, Map<String, Setting<?>>> cacheStoreTypeMap : entry.getValue().entrySet()) {
                for (Map.Entry<String, Setting<?>> entry1 : cacheStoreTypeMap.getValue().entrySet()) {
                    settingList.add(entry1.getValue());
                }
            }
        }
        return settingList;
    }

    @Override
    public String getName() {
        return EHCACHE_CACHE_PLUGIN;
    }
}
