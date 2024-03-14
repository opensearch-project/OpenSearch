/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plugin for TieredSpilloverCache.
 */
public class TieredSpilloverCachePlugin extends Plugin implements CachePlugin {

    /**
     * Plugin name
     */
    public static final String TIERED_CACHE_SPILLOVER_PLUGIN_NAME = "tieredSpilloverCachePlugin";

    /**
     * Default constructor
     */
    public TieredSpilloverCachePlugin() {}

    @Override
    public Map<String, ICache.Factory> getCacheFactoryMap() {
        return Map.of(
            TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME,
            new TieredSpilloverCache.TieredSpilloverCacheFactory()
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        for (CacheType cacheType : CacheType.values()) {
            settingList.add(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
            );
            settingList.add(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORE_NAME.getConcreteSettingForNamespace(cacheType.getSettingPrefix())
            );
        }
        return settingList;
    }

    @Override
    public String getName() {
        return TIERED_CACHE_SPILLOVER_PLUGIN_NAME;
    }
}
