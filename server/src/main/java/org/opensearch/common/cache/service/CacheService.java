/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.service;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Service responsible to create caches.
 */
@ExperimentalApi
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
        if (!FeatureFlags.PLUGGABLE_CACHE_SETTING.get(settings) || (storeName == null || storeName.isBlank())) {
            // Condition 1: In case feature flag is off, we default to onHeap.
            // Condition 2: In case storeName is not explicitly mentioned, we assume user is looking to use older
            // settings, so we again fallback to onHeap to maintain backward compatibility.
            // It is guaranteed that we will have this store name registered, so
            // should be safe.
            storeName = OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME;
        }
        if (!cacheStoreTypeFactories.containsKey(storeName)) {
            throw new IllegalArgumentException("No store name: [" + storeName + "] is registered for cache type: " + cacheType);
        }
        ICache.Factory factory = cacheStoreTypeFactories.get(storeName);
        ICache<K, V> iCache = factory.create(config, cacheType, cacheStoreTypeFactories);
        cacheTypeMap.put(cacheType, iCache);
        return iCache;
    }

    public NodeCacheStats stats(CommonStatsFlags flags) {
        final SortedMap<CacheType, ImmutableCacheStatsHolder> statsMap = new TreeMap<>();
        for (CacheType type : cacheTypeMap.keySet()) {
            statsMap.put(type, cacheTypeMap.get(type).stats(flags.getLevels()));
        }
        return new NodeCacheStats(statsMap, flags);
    }
}
