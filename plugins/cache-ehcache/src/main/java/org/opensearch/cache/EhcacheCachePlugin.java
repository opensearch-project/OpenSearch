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
import org.opensearch.common.cache.ICache;
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.cache.EhcacheDiskCacheSettings.CACHE_TYPE_MAP;

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
    public Map<String, ICache.Factory> getCacheFactoryMap() {
        return Map.of(EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME, new EhcacheDiskCache.EhcacheDiskCacheFactory());
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        for (Map.Entry<CacheType, Map<String, Setting<?>>> entry : CACHE_TYPE_MAP.entrySet()) {
            for (Map.Entry<String, Setting<?>> entry1 : entry.getValue().entrySet()) {
                settingList.add(entry1.getValue());
            }
        }
        return settingList;
    }

    @Override
    public String getName() {
        return EHCACHE_CACHE_PLUGIN;
    }
}
