/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.provider;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheProviderTests extends OpenSearchTestCase {

    public void testWithMultiplePlugins() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        StoreAwareCache.Factory factory1 = mock(StoreAwareCache.Factory.class);
        CachePlugin mockPlugin2 = mock(CachePlugin.class);
        StoreAwareCache.Factory factory2 = mock(StoreAwareCache.Factory.class);
        when(factory1.getCacheName()).thenReturn("cache1");
        when(factory2.getCacheName()).thenReturn("cache2");
        when(mockPlugin1.getCacheStoreTypeMap()).thenReturn(Map.of(CacheStoreType.DISK, factory1, CacheStoreType.ON_HEAP, factory1));
        when(mockPlugin2.getCacheStoreTypeMap()).thenReturn(Map.of(CacheStoreType.DISK, factory2, CacheStoreType.ON_HEAP, factory2));

        CacheProvider cacheProvider = new CacheProvider(List.of(mockPlugin1, mockPlugin2), Settings.EMPTY);

        Map<CacheStoreType, Map<String, StoreAwareCache.Factory>> cacheStoreTypeListMap = cacheProvider.getCacheStoreTypeFactories();
        assertNotNull(cacheStoreTypeListMap.get(CacheStoreType.DISK).get("cache1"));
        assertNotNull(cacheStoreTypeListMap.get(CacheStoreType.DISK).get("cache2"));
        assertNotNull(cacheStoreTypeListMap.get(CacheStoreType.ON_HEAP).get("cache1"));
        assertNotNull(cacheStoreTypeListMap.get(CacheStoreType.ON_HEAP).get("cache2"));
    }

    public void testWithSameCacheStoreTypeAndName() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        StoreAwareCache.Factory factory1 = mock(StoreAwareCache.Factory.class);
        CachePlugin mockPlugin2 = mock(CachePlugin.class);
        StoreAwareCache.Factory factory2 = mock(StoreAwareCache.Factory.class);
        when(factory1.getCacheName()).thenReturn("cache");
        when(factory2.getCacheName()).thenReturn("cache");
        when(mockPlugin1.getCacheStoreTypeMap()).thenReturn(Map.of(CacheStoreType.DISK, factory1));
        when(mockPlugin2.getCacheStoreTypeMap()).thenReturn(Map.of(CacheStoreType.DISK, factory2));

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new CacheProvider(List.of(mockPlugin1, mockPlugin2), Settings.EMPTY)
        );
        assertEquals("Cache name: cache is already registered for store type: DISK", ex.getMessage());
    }

    public void testWithCacheFactoryPresentForCacheType() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        StoreAwareCache.Factory factory1 = mock(StoreAwareCache.Factory.class);
        when(factory1.getCacheName()).thenReturn("cache1");
        when(mockPlugin1.getCacheStoreTypeMap()).thenReturn(Map.of(CacheStoreType.DISK, factory1));

        CacheProvider cacheProvider = new CacheProvider(
            List.of(mockPlugin1),
            Settings.builder().put(CacheType.INDICES_REQUEST_CACHE.getSettingPrefix() + ".disk.store.name", "cache1").build()
        );
        assertTrue(cacheProvider.getStoreAwareCacheForCacheType(CacheStoreType.DISK, CacheType.INDICES_REQUEST_CACHE).isPresent());
    }

    public void testWithCacheFactoryNotPresentForCacheType() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        StoreAwareCache.Factory factory1 = mock(StoreAwareCache.Factory.class);
        when(factory1.getCacheName()).thenReturn("cache1");
        when(mockPlugin1.getCacheStoreTypeMap()).thenReturn(Map.of(CacheStoreType.DISK, factory1));

        CacheProvider cacheProvider = new CacheProvider(
            List.of(mockPlugin1),
            Settings.builder().put(CacheType.INDICES_REQUEST_CACHE.getSettingPrefix() + ".disk.store.name", "cache2").build()
        );
        assertFalse(cacheProvider.getStoreAwareCacheForCacheType(CacheStoreType.DISK, CacheType.INDICES_REQUEST_CACHE).isPresent());

        assertFalse(cacheProvider.getStoreAwareCacheForCacheType(CacheStoreType.ON_HEAP, CacheType.INDICES_REQUEST_CACHE).isPresent());
    }

    public void testWithNoStoreNameForCacheType() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        StoreAwareCache.Factory factory1 = mock(StoreAwareCache.Factory.class);
        when(factory1.getCacheName()).thenReturn("cache1");
        when(mockPlugin1.getCacheStoreTypeMap()).thenReturn(Map.of(CacheStoreType.DISK, factory1));

        CacheProvider cacheProvider = new CacheProvider(List.of(mockPlugin1), Settings.EMPTY);
        assertFalse(cacheProvider.getStoreAwareCacheForCacheType(CacheStoreType.DISK, CacheType.INDICES_REQUEST_CACHE).isPresent());
    }
}
