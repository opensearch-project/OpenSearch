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

        when(mockPlugin1.getCacheStoreTypeMap()).thenReturn(Map.of(CacheStoreType.DISK, factory1, CacheStoreType.ON_HEAP, factory1));
        when(mockPlugin2.getCacheStoreTypeMap()).thenReturn(Map.of(CacheStoreType.DISK, factory2, CacheStoreType.ON_HEAP, factory2));

        CacheProvider cacheProvider = new CacheProvider(List.of(mockPlugin1, mockPlugin2), Settings.EMPTY);

        Map<CacheStoreType, List<StoreAwareCache.Factory>> cacheStoreTypeListMap = cacheProvider.getCacheStoreTypeFactories();
        assertEquals(2, cacheStoreTypeListMap.get(CacheStoreType.DISK).size());
        assertEquals(2, cacheStoreTypeListMap.get(CacheStoreType.ON_HEAP).size());
    }
}
