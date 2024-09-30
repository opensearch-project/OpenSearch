/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

public class TieredSpilloverCacheBaseIT extends OpenSearchIntegTestCase {

    public Settings defaultSettings(String onHeapCacheSizeInBytesOrPercentage, int numberOfSegments) {
        return Settings.builder()
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .put(
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                MockDiskCache.MockDiskCacheFactory.NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_SEGMENTS.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                numberOfSegments
            )
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                onHeapCacheSizeInBytesOrPercentage
            )
            .build();
    }

    public int getNumberOfSegments() {
        return randomFrom(1, 2, 4, 8, 16, 64, 128, 256);
    }
}
