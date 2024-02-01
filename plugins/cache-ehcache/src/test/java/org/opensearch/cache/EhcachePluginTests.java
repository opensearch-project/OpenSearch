/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class EhcachePluginTests extends OpenSearchTestCase {

    private EhcacheCachePlugin ehcacheCachePlugin = new EhcacheCachePlugin();

    public void testGetCacheStoreTypeMap() {
        Map<CacheStoreType, StoreAwareCache.Factory> factoryMap = ehcacheCachePlugin.getCacheStoreTypeMap();
        assertNotNull(factoryMap);
        assertNotNull(factoryMap.get(CacheStoreType.DISK));
    }
}
