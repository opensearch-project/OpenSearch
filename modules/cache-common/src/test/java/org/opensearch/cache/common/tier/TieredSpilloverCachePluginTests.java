/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.provider.CacheProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;

public class TieredSpilloverCachePluginTests extends OpenSearchTestCase {

    public void testGetCacheFactoryMap() {
        TieredSpilloverCachePlugin tieredSpilloverCachePlugin = new TieredSpilloverCachePlugin();
        Map<String, ICache.Factory> map = tieredSpilloverCachePlugin.getCacheFactoryMap(mock(CacheProvider.class));
        assertNotNull(map.get(TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME));
        assertEquals(TieredSpilloverCachePlugin.TIERED_CACHE_SPILLOVER_PLUGIN_NAME, tieredSpilloverCachePlugin.getName());
    }
}
