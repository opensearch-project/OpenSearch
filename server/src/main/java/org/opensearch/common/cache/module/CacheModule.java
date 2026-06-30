/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.module;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.service.CacheService;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CachePlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds all the cache factories and provides a way to fetch them when needed.
 */
@ExperimentalApi
public class CacheModule {

    private final Map<String, ICache.Factory> cacheStoreTypeFactories;

    private final CacheService cacheService;
    private final Settings settings;

    public CacheModule(List<CachePlugin> cachePlugins, Settings settings) {
        this.cacheStoreTypeFactories = getCacheStoreTypeFactories(cachePlugins);
        this.settings = settings;
        this.cacheService = new CacheService(cacheStoreTypeFactories, settings);
    }

    private static Map<String, ICache.Factory> getCacheStoreTypeFactories(List<CachePlugin> cachePlugins) {
        Map<String, ICache.Factory> cacheStoreTypeFactories = new HashMap<>();
        // Add the core OpenSearchOnHeapCache as well.
        cacheStoreTypeFactories.put(
            OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME,
            new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory()
        );
        for (CachePlugin cachePlugin : cachePlugins) {
            Map<String, ICache.Factory> factoryMap = cachePlugin.getCacheFactoryMap();
            for (Map.Entry<String, ICache.Factory> entry : factoryMap.entrySet()) {
                if (cacheStoreTypeFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Cache name: " + entry.getKey() + " is " + "already registered");
                }
            }
        }
        return Collections.unmodifiableMap(cacheStoreTypeFactories);
    }

    public CacheService getCacheService() {
        return this.cacheService;
    }

    // Package private for testing.
    Map<String, ICache.Factory> getCacheStoreTypeFactories() {
        return cacheStoreTypeFactories;
    }
}
