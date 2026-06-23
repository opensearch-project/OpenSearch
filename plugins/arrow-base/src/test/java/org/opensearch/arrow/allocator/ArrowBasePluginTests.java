/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

public class ArrowBasePluginTests extends OpenSearchTestCase {

    public void testQuerySettingsExposeDefaults() {
        // Explicit 0 expresses "AC unconfigured" so QUERY_MAX falls back to Long.MAX_VALUE.
        Settings s = Settings.builder().put("node.native_memory.limit", "0b").build();
        assertEquals(Long.valueOf(0L), ArrowBasePlugin.QUERY_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testFlightAndIngestMinDerivedFromBudget() {
        // With node.native_memory.limit set, mins derive as percentages. No warm role here, so
        // ingest uses the non-warm default.
        Settings s = Settings.builder().put("node.native_memory.limit", "1gb").build();
        long budget = 1024L * 1024 * 1024;
        // flight min = 2% of budget, ingest min = 4% of budget (non-warm)
        assertEquals(Long.valueOf(budget * 2 / 100), ArrowBasePlugin.FLIGHT_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(budget * 4 / 100), ArrowBasePlugin.INGEST_MIN_SETTING.get(s));
    }

    public void testPoolMaxDefaultsAreLongMaxValueWhenAcUnset() {
        Settings s = Settings.builder().put("node.native_memory.limit", "0b").build();
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.FLIGHT_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.INGEST_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testPoolMaxDefaultsScaleFromAcBudget() {
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").build();
        long limit = 10L * 1024 * 1024 * 1024;
        assertEquals(Long.valueOf(limit * 5 / 100), ArrowBasePlugin.FLIGHT_MAX_SETTING.get(s));
        // No warm role, so ingest uses the non-warm default (8%).
        assertEquals(Long.valueOf(limit * 8 / 100), ArrowBasePlugin.INGEST_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(limit * 5 / 100), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testIngestPoolDefaultsAreReducedOnWarmNodes() {
        // Warm nodes shrink the ingest pool (min 2%, max 4%) to free budget for the metadata cache.
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").putList("node.roles", "warm").build();
        long limit = 10L * 1024 * 1024 * 1024;
        assertEquals(Long.valueOf(limit * 2 / 100), ArrowBasePlugin.INGEST_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(limit * 4 / 100), ArrowBasePlugin.INGEST_MAX_SETTING.get(s));
    }

    public void testPoolMaxDefaultsIgnoreBufferPercent() {
        Settings s = Settings.builder().put("node.native_memory.limit", "1000b").put("node.native_memory.buffer_percent", 20).build();
        assertEquals(Long.valueOf(50L), ArrowBasePlugin.FLIGHT_MAX_SETTING.get(s));
        // No warm role, so ingest uses the non-warm default (8% of 1000 = 80).
        assertEquals(Long.valueOf(80L), ArrowBasePlugin.INGEST_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(50L), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testPoolMaxRejectsNegative() {
        Settings flight = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, -1L).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ArrowBasePlugin.FLIGHT_MAX_SETTING.get(flight));
        assertTrue(e.getMessage().contains("must be >= 0"));
        assertTrue("flight error must name its setting", e.getMessage().contains(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX));

        Settings ingest = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, -1L).build();
        IllegalArgumentException eIngest = expectThrows(
            IllegalArgumentException.class,
            () -> ArrowBasePlugin.INGEST_MAX_SETTING.get(ingest)
        );
        assertTrue(eIngest.getMessage().contains("must be >= 0"));
        assertTrue("ingest error must name its setting", eIngest.getMessage().contains(NativeAllocatorPoolConfig.SETTING_INGEST_MAX));

        Settings query = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, -1L).build();
        IllegalArgumentException eQuery = expectThrows(IllegalArgumentException.class, () -> ArrowBasePlugin.QUERY_MAX_SETTING.get(query));
        assertTrue(eQuery.getMessage().contains("must be >= 0"));
        assertTrue("query error must name its setting", eQuery.getMessage().contains(NativeAllocatorPoolConfig.SETTING_QUERY_MAX));
    }

    // -----------------------------------------------------------------
    // End-to-end wiring tests
    // -----------------------------------------------------------------

    private static ClusterSettings newClusterSettings(Settings nodeSettings) {
        Set<Setting<?>> registered = new HashSet<>();
        registered.addAll(new ArrowBasePlugin().getSettings());
        return new ClusterSettings(nodeSettings, registered);
    }

    public void testBuildAllocatorWiresAllPools() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put("node.native_memory.limit", "10gb")
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, 1L * 1024 * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, 2L * 1024 * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 1L * 1024 * 1024 * 1024)
            .put("native.allocator.rebalancer.enabled", false)
            .build();
        ClusterSettings cs = newClusterSettings(nodeSettings);

        ArrowBasePlugin plugin = new ArrowBasePlugin();
        long budget = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(nodeSettings).getBytes();
        ArrowNativeAllocator allocator = plugin.buildAllocator(nodeSettings, cs, () -> budget);
        try {
            Set<String> poolNames = allocator.getPoolNames();
            assertEquals("buildAllocator must register exactly the framework's three pools", 3, poolNames.size());
            assertTrue(poolNames.contains(NativeAllocatorPoolConfig.POOL_FLIGHT));
            assertTrue(poolNames.contains(NativeAllocatorPoolConfig.POOL_INGEST));
            assertTrue(poolNames.contains(NativeAllocatorPoolConfig.POOL_QUERY));

            // Pool maxes match the operator-set values (rebalancer disabled,
            // so initial limit == max).
            assertEquals(1L * 1024 * 1024 * 1024, allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT).getLimit());
            assertEquals(2L * 1024 * 1024 * 1024, allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST).getLimit());
            assertEquals(1L * 1024 * 1024 * 1024, allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY).getLimit());
        } finally {
            allocator.close();
            plugin.close();
        }
    }

    public void testBuildAllocatorWithRebalancerPoolsStartAtMax() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put("node.native_memory.limit", "1gb")
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN, 10L * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, 200L * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MIN, 20L * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, 200L * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MIN, 10L * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 200L * 1024 * 1024)
            .put("native.allocator.rebalancer.enabled", true)
            .put("native.allocator.rebalance.interval_seconds", 5L)
            .build();
        ClusterSettings cs = newClusterSettings(nodeSettings);

        ArrowBasePlugin plugin = new ArrowBasePlugin();
        long budget2 = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(nodeSettings).getBytes();
        ArrowNativeAllocator allocator = plugin.buildAllocator(nodeSettings, cs, () -> budget2);
        try {
            // Pools always start at max regardless of rebalancer state
            assertEquals(200L * 1024 * 1024, allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT).getLimit());
            assertEquals(200L * 1024 * 1024, allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST).getLimit());
            assertEquals(200L * 1024 * 1024, allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY).getLimit());
        } finally {
            allocator.close();
            plugin.close();
        }
    }

    public void testRebalanceIntervalSettingDefault() {
        assertEquals(Long.valueOf(5L), ArrowBasePlugin.REBALANCE_INTERVAL_SETTING.get(Settings.EMPTY));
    }
}
