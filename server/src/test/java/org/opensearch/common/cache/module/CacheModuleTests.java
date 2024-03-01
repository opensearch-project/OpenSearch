/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.module;

import org.opensearch.common.cache.ICache;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheModuleTests extends OpenSearchTestCase {

    public void testWithMultiplePlugins() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        CachePlugin mockPlugin2 = mock(CachePlugin.class);
        ICache.Factory factory2 = mock(ICache.Factory.class);
        when(mockPlugin1.getCacheFactoryMap()).thenReturn(Map.of("cache1", factory1));
        when(mockPlugin2.getCacheFactoryMap()).thenReturn(Map.of("cache2", factory2));

        CacheModule cacheModule = new CacheModule(List.of(mockPlugin1, mockPlugin2), Settings.EMPTY);

        Map<String, ICache.Factory> factoryMap = cacheModule.getCacheStoreTypeFactories();
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
        when(mockPlugin1.getCacheFactoryMap()).thenReturn(Map.of("cache", factory1));
        when(mockPlugin2.getCacheFactoryMap()).thenReturn(Map.of("cache", factory2));

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new CacheModule(List.of(mockPlugin1, mockPlugin2), Settings.EMPTY)
        );
        assertEquals("Cache name: cache is already registered", ex.getMessage());
    }
}
