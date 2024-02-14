/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.provider;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
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
@ExperimentalApi
public class CacheProvider {

    private final Map<String, ICache.Factory> cacheStoreTypeFactories;
    private final Settings settings;

    public CacheProvider(List<CachePlugin> cachePlugins, Settings settings) {
        this.cacheStoreTypeFactories = getCacheStoreTypeFactories(cachePlugins);
        this.settings = settings;
    }

    private Map<String, ICache.Factory> getCacheStoreTypeFactories(List<CachePlugin> cachePlugins) {
        Map<String, ICache.Factory> cacheStoreTypeFactories = new HashMap<>();
        for (CachePlugin cachePlugin : cachePlugins) {
            Map<String, ICache.Factory> factoryMap = cachePlugin.getCacheFactoryMap(this);
            for (Map.Entry<String, ICache.Factory> entry : factoryMap.entrySet()) {
                if (cacheStoreTypeFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Cache name: " + entry.getKey() + " is " + "already registered");
                }
            }
        }
        // Add the core OpenSearchOnHeapCache as well.
        cacheStoreTypeFactories.put(
            OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME,
            new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory()
        );
        return Collections.unmodifiableMap(cacheStoreTypeFactories);
    }

    // Package private for testing.
    protected Map<String, ICache.Factory> getCacheStoreTypeFactories() {
        return cacheStoreTypeFactories;
    }

    public Optional<ICache.Factory> getCacheFactoryForCacheStoreName(String cacheStoreName) {
        if (cacheStoreName == null || cacheStoreName.isBlank()) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(cacheStoreTypeFactories.get(cacheStoreName));
        }
    }

    public Optional<ICache.Factory> getCacheFactoryForCacheType(CacheType cacheType) {
        Setting<String> cacheSettingForCacheType = CacheSettings.CACHE_TYPE_STORE_NAME.getConcreteSettingForNamespace(
            cacheType.getSettingPrefix()
        );
        String storeName = cacheSettingForCacheType.get(settings);
        if (storeName == null || storeName.isBlank()) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(cacheStoreTypeFactories.get(storeName));
        }
    }
}
