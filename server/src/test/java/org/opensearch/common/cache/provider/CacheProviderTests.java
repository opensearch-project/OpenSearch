/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.provider;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheProviderTests extends OpenSearchTestCase {

    public void testWithMultiplePlugins() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        CachePlugin mockPlugin2 = mock(CachePlugin.class);
        ICache.Factory factory2 = mock(ICache.Factory.class);
        when(mockPlugin1.getCacheFactoryMap(any(CacheProvider.class))).thenReturn(Map.of("cache1", factory1));
        when(mockPlugin2.getCacheFactoryMap(any(CacheProvider.class))).thenReturn(Map.of("cache2", factory2));

        CacheProvider cacheProvider = new CacheProvider(List.of(mockPlugin1, mockPlugin2), Settings.EMPTY);

        Map<String, ICache.Factory> factoryMap = cacheProvider.getCacheStoreTypeFactories();
        assertEquals(factoryMap.get("cache1"), factory1);
        assertEquals(factoryMap.get("cache2"), factory2);
    }

    public void testWithSameCacheStoreTypeAndName() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        CachePlugin mockPlugin2 = mock(CachePlugin.class);
        ICache.Factory factory2 = mock(ICache.Factory.class);
        when(factory1.getCacheName()).thenReturn("cache");
        when(factory2.getCacheName()).thenReturn("cache");
        when(mockPlugin1.getCacheFactoryMap(any(CacheProvider.class))).thenReturn(Map.of("cache", factory1));
        when(mockPlugin2.getCacheFactoryMap(any(CacheProvider.class))).thenReturn(Map.of("cache", factory2));

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new CacheProvider(List.of(mockPlugin1, mockPlugin2), Settings.EMPTY)
        );
        assertEquals("Cache name: cache is already registered", ex.getMessage());
    }

    public void testWithCacheFactoryPresentForIndicesRequestCacheType() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        when(mockPlugin1.getCacheFactoryMap(any(CacheProvider.class))).thenReturn(Map.of("cache1", factory1));

        Setting<String> indicesRequestCacheSetting = CacheSettings.getConcreteSettingForCacheType(CacheType.INDICES_REQUEST_CACHE);

        CacheProvider cacheProvider = new CacheProvider(
            List.of(mockPlugin1),
            Settings.builder().put(indicesRequestCacheSetting.getKey(), "cache1").build()
        );
        assertTrue(cacheProvider.getCacheFactoryForCacheType(CacheType.INDICES_REQUEST_CACHE).isPresent());
        assertEquals(cacheProvider.getCacheFactoryForCacheType(CacheType.INDICES_REQUEST_CACHE).get(), factory1);
    }

    public void testWithCacheFactoryNotPresentForIndicesRequestCacheType() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        when(mockPlugin1.getCacheFactoryMap(any(CacheProvider.class))).thenReturn(Map.of("cache1", factory1));

        Setting<String> indicesRequestCacheSetting = CacheSettings.getConcreteSettingForCacheType(CacheType.INDICES_REQUEST_CACHE);

        CacheProvider cacheProvider = new CacheProvider(
            List.of(mockPlugin1),
            Settings.builder().put(indicesRequestCacheSetting.getKey(), "cache").build()
        );
        assertFalse(cacheProvider.getCacheFactoryForCacheType(CacheType.INDICES_REQUEST_CACHE).isPresent());
    }

    public void testGetCacheFactoryForCacheStoreName() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        when(mockPlugin1.getCacheFactoryMap(any(CacheProvider.class))).thenReturn(Map.of("cache1", factory1));

        CacheProvider cacheProvider = new CacheProvider(List.of(mockPlugin1), Settings.EMPTY);
        assertTrue(cacheProvider.getCacheFactoryForCacheStoreName("cache1").isPresent());
        assertFalse(cacheProvider.getCacheFactoryForCacheStoreName("cache").isPresent());
    }
}
