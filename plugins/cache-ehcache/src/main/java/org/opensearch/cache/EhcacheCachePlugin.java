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
import org.opensearch.plugins.CachePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Map;

/**
 * Ehcache based cache plugin.
 */
public class EhcacheCachePlugin extends Plugin implements CachePlugin {

    private static final String EHCACHE_CACHE_PLUGIN = "EhcachePlugin";

    /**
     * Default constructor to avoid javadoc related failures.
     */
    public EhcacheCachePlugin() {}

    @Override
    public Map<CacheStoreType, StoreAwareCache.Factory> getCacheStoreTypeMap() {
        return Map.of(CacheStoreType.DISK, new EhcacheDiskCache.EhcacheDiskCacheFactory());
    }

    @Override
    public String getName() {
        return EHCACHE_CACHE_PLUGIN;
    }
}
