/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.ICache;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class TieredSpilloverCachePluginTests extends OpenSearchTestCase {

    public void testGetCacheFactoryMap() {
        TieredSpilloverCachePlugin tieredSpilloverCachePlugin = new TieredSpilloverCachePlugin(Settings.EMPTY);
        Map<String, ICache.Factory> map = tieredSpilloverCachePlugin.getCacheFactoryMap();
        assertNotNull(map.get(TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME));
        assertEquals(TieredSpilloverCachePlugin.TIERED_CACHE_SPILLOVER_PLUGIN_NAME, tieredSpilloverCachePlugin.getName());
    }

    public void testGetSettingsWithFeatureFlagOn() {
        TieredSpilloverCachePlugin tieredSpilloverCachePlugin = new TieredSpilloverCachePlugin(
            Settings.builder().put(FeatureFlags.PLUGGABLE_CACHE_SETTING.getKey(), true).build()
        );
        assertFalse(tieredSpilloverCachePlugin.getSettings().isEmpty());
    }
}
