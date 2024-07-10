/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import org.opensearch.common.cache.ICache;
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.plugins.Plugin;

import java.util.List;
import java.util.Map;


public class CaffeineCachePlugin extends Plugin implements CachePlugin {

    private static final String CAFFEINE_CACHE_PLUGIN = "CaffeinePlugin";

    /**
     * Default constructor to avoid javadoc related failures.
     */
    public CaffeineCachePlugin() {}

    @Override
    public Map<String, ICache.Factory> getCacheFactoryMap() {
        return null;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return null;
    }

    @Override
    public String getName() {
        return CAFFEINE_CACHE_PLUGIN;
    }
}
