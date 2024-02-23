/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.service;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

/**
 * Service responsible to create caches.
 */
public class CacheService {

    private final Map<String, ICache.Factory> cacheStoreTypeFactories;
    private final Settings settings;
    private Map<CacheType, ICache<?, ?>> cacheTypeMap;

    public CacheService(Map<String, ICache.Factory> cacheStoreTypeFactories, Settings settings) {
        this.cacheStoreTypeFactories = cacheStoreTypeFactories;
        this.settings = settings;
        this.cacheTypeMap = new HashMap<>();
    }

    public Map<CacheType, ICache<?, ?>> getCacheTypeMap() {
        return this.cacheTypeMap;
    }

    public <K, V> ICache<K, V> createCache(CacheConfig<K, V> config, CacheType cacheType) {
        Setting<String> cacheSettingForCacheType = CacheSettings.CACHE_TYPE_STORE_NAME.getConcreteSettingForNamespace(
            cacheType.getSettingPrefix()
        );
        String storeName = cacheSettingForCacheType.get(settings);
        if (storeName == null || storeName.isBlank()) {
            throw new IllegalArgumentException("No configuration exists for cache type: " + cacheType);
        }
        if (!cacheStoreTypeFactories.containsKey(storeName)) {
            throw new IllegalArgumentException("No store name: [" + storeName + "] is registered for cache type: " + cacheType);
        }
        ICache.Factory factory = cacheStoreTypeFactories.get(storeName);
        ICache<K, V> iCache = factory.create(config, cacheType, cacheStoreTypeFactories);
        cacheTypeMap.put(cacheType, iCache);
        return iCache;
    }
}
