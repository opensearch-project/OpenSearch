/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.provider;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CachePlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Holds all the cache factories and provides a way to fetch them when needed.
 */
public class CacheProvider {

    private final Map<CacheStoreType, Map<String, StoreAwareCache.Factory>> cacheStoreTypeFactories;
    private final Settings settings;

    public CacheProvider(List<CachePlugin> cachePlugins, Settings settings) {
        this.cacheStoreTypeFactories = getCacheStoreTypeFactories(cachePlugins);
        this.settings = settings;
    }

    private Map<CacheStoreType, Map<String, StoreAwareCache.Factory>> getCacheStoreTypeFactories(List<CachePlugin> cachePlugins) {
        Map<CacheStoreType, Map<String, StoreAwareCache.Factory>> cacheStoreTypeFactories = new HashMap<>();
        for (CachePlugin cachePlugin : cachePlugins) {
            Map<CacheStoreType, StoreAwareCache.Factory> factoryMap = cachePlugin.getCacheStoreTypeMap();
            for (Map.Entry<CacheStoreType, StoreAwareCache.Factory> entry : factoryMap.entrySet()) {
                if (cacheStoreTypeFactories.computeIfAbsent(entry.getKey(), k -> new HashMap<>())
                    .putIfAbsent(entry.getValue().getCacheName(), entry.getValue()) != null) {
                    throw new IllegalArgumentException(
                        "Cache name: " + entry.getValue().getCacheName() + " is " + "already registered for store type: " + entry.getKey()
                    );
                }
            }
        }
        return Collections.unmodifiableMap(cacheStoreTypeFactories);
    }

    // Package private for testing.
    protected Map<CacheStoreType, Map<String, StoreAwareCache.Factory>> getCacheStoreTypeFactories() {
        return cacheStoreTypeFactories;
    }

    public Optional<StoreAwareCache.Factory> getStoreAwareCacheForCacheType(CacheStoreType cacheStoreType, CacheType cacheType) {
        if (!cacheStoreTypeFactories.containsKey(cacheStoreType) || cacheStoreTypeFactories.get(cacheStoreType).isEmpty()) {
            return Optional.empty();
        }

        Setting<String> cacheSettingForCacheType = CacheSettings.getConcreteSettingForCacheType(cacheType, cacheStoreType);
        String storeName = cacheSettingForCacheType.get(settings);
        if (storeName == null || storeName.isBlank()) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(cacheStoreTypeFactories.get(cacheStoreType).get(storeName));
        }
    }
}
