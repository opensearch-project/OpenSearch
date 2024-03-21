/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.TimeUnit;

import static org.opensearch.common.settings.Setting.Property.NodeScope;

/**
 * Settings related to TieredSpilloverCache.
 */
public class TieredSpilloverCacheSettings {

    /**
     * Setting which defines the onHeap cache store to be used in TieredSpilloverCache.
     *
     * Pattern: {cache_type}.tiered_spillover.onheap.store.name
     * Example: indices.request.cache.tiered_spillover.onheap.store.name
     */
    public static final Setting.AffixSetting<String> TIERED_SPILLOVER_ONHEAP_STORE_NAME = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".onheap.store.name",
        (key) -> Setting.simpleString(key, "", NodeScope)
    );

    /**
     * Setting which defines the disk cache store to be used in TieredSpilloverCache.
     */
    public static final Setting.AffixSetting<String> TIERED_SPILLOVER_DISK_STORE_NAME = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".disk.store.name",
        (key) -> Setting.simpleString(key, "", NodeScope)
    );

    /**
     * Setting defining the minimum took time for a query to be allowed into the disk cache.
     */
    public static final Setting.AffixSetting<TimeValue> TIERED_SPILLOVER_DISK_TOOK_TIME_THRESHOLD = Setting.suffixKeySetting(
        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME + ".disk.store.policies.took_time.threshold",
        (key) -> Setting.timeSetting(
            key,
            new TimeValue(10, TimeUnit.MILLISECONDS), // Default value for this setting
            TimeValue.ZERO, // Minimum value for this setting
            NodeScope
        )
    );
    // 10 ms was chosen as a safe value based on proof of concept, where we saw disk latencies in this range.
    // Will be tuned further with future benchmarks.

    /**
     * Default constructor
     */
    TieredSpilloverCacheSettings() {}
}
