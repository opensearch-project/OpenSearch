/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import org.opensearch.cache.store.disk.EhcacheDiskCache;
import org.opensearch.common.cache.ICache;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class EhcachePluginTests extends OpenSearchTestCase {

    private EhcacheCachePlugin ehcacheCachePlugin = new EhcacheCachePlugin();

    public void testGetCacheStoreTypeMap() {
        Map<String, ICache.Factory> factoryMap = ehcacheCachePlugin.getCacheFactoryMap();
        assertNotNull(factoryMap);
        assertNotNull(factoryMap.get(EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME));
    }
}
