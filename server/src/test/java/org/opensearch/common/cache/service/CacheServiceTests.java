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
import org.opensearch.common.cache.module.CacheModule;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheServiceTests extends OpenSearchTestCase {

    public void testWithCreateCacheForIndicesRequestCacheType() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);
        when(mockPlugin1.getCacheFactoryMap()).thenReturn(factoryMap);

        Setting<String> indicesRequestCacheSetting = CacheSettings.getConcreteSettingForCacheType(CacheType.INDICES_REQUEST_CACHE);

        CacheModule cacheModule = new CacheModule(
            List.of(mockPlugin1),
            Settings.builder().put(indicesRequestCacheSetting.getKey(), "cache1").build()
        );
        CacheConfig<String, String> config = mock(CacheConfig.class);
        ICache<String, String> onHeapCache = mock(OpenSearchOnHeapCache.class);
        when(factory1.create(eq(config), eq(CacheType.INDICES_REQUEST_CACHE), any(Map.class))).thenReturn(onHeapCache);

        CacheService cacheService = cacheModule.getCacheService();
        ICache<String, String> ircCache = cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE);
        assertEquals(onHeapCache, ircCache);
    }

    public void testWithCreateCacheWithNoStoreNamePresentForCacheType() {
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);
        CacheService cacheService = new CacheService(factoryMap, Settings.builder().build());

        CacheConfig<String, String> config = mock(CacheConfig.class);
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE)
        );
        assertEquals("No configuration exists for cache type: INDICES_REQUEST_CACHE", ex.getMessage());
    }

    public void testWithCreateCacheWithInvalidStoreNameAssociatedForCacheType() {
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Setting<String> indicesRequestCacheSetting = CacheSettings.getConcreteSettingForCacheType(CacheType.INDICES_REQUEST_CACHE);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);
        CacheService cacheService = new CacheService(
            factoryMap,
            Settings.builder().put(indicesRequestCacheSetting.getKey(), "cache").build()
        );

        CacheConfig<String, String> config = mock(CacheConfig.class);
        ICache<String, String> onHeapCache = mock(OpenSearchOnHeapCache.class);
        when(factory1.create(config, CacheType.INDICES_REQUEST_CACHE, factoryMap)).thenReturn(onHeapCache);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE)
        );
        assertEquals("No store name: [cache] is registered for cache type: INDICES_REQUEST_CACHE", ex.getMessage());
    }
}
