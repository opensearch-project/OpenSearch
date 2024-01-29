/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.provider;

import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CachePlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds all the cache factories and provides a way to fetch them when needed.
 */
public class CacheProvider {

    private final Map<CacheStoreType, List<StoreAwareCache.Factory>> cacheStoreTypeFactories;

    private final Settings settings;

    public CacheProvider(List<CachePlugin> cachePlugins, Settings settings) {
        this.cacheStoreTypeFactories = getCacheStoreTypeFactories(cachePlugins);
        this.settings = settings;
    }

    private Map<CacheStoreType, List<StoreAwareCache.Factory>> getCacheStoreTypeFactories(List<CachePlugin> cachePlugins) {
        Map<CacheStoreType, List<StoreAwareCache.Factory>> cacheStoreTypeFactories = new HashMap<>();
        for (CachePlugin cachePlugin : cachePlugins) {
            Map<CacheStoreType, StoreAwareCache.Factory> factoryMap = cachePlugin.getCacheStoreTypeMap();
            for (Map.Entry<CacheStoreType, StoreAwareCache.Factory> entry : factoryMap.entrySet()) {
                cacheStoreTypeFactories.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
            }
        }
        return Collections.unmodifiableMap(cacheStoreTypeFactories);
    }

    public Map<CacheStoreType, List<StoreAwareCache.Factory>> getCacheStoreTypeFactories() {
        return cacheStoreTypeFactories;
    }
}
