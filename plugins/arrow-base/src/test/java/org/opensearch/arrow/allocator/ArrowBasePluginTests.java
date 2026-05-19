/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

public class ArrowBasePluginTests extends OpenSearchTestCase {

    public void testDeriveRootLimitDefaultUnsetReturnsLongMaxValue() {
        Settings s = Settings.EMPTY;
        assertEquals(Long.toString(Long.MAX_VALUE), ArrowBasePlugin.deriveRootLimitDefault(s));
    }

    public void testDeriveRootLimitDefaultUsesAcLimitWhenSet() {
        Settings s = Settings.builder().put("node.native_memory.limit", "1gb").build();
        // 1 GiB == 2^30 bytes; buffer_percent default 0 => full budget.
        assertEquals(Long.toString(1024L * 1024 * 1024), ArrowBasePlugin.deriveRootLimitDefault(s));
    }

    public void testDeriveRootLimitDefaultAppliesBufferPercent() {
        Settings s = Settings.builder()
            .put("node.native_memory.limit", "1000b")
            .put("node.native_memory.buffer_percent", 20)
            .build();
        // 1000 - (1000 * 20 / 100) = 800
        assertEquals("800", ArrowBasePlugin.deriveRootLimitDefault(s));
    }

    public void testRootLimitSettingExposesDerivedDefault() {
        Settings s = Settings.builder().put("node.native_memory.limit", "2gb").build();
        assertEquals(Long.valueOf(2L * 1024 * 1024 * 1024), ArrowBasePlugin.ROOT_LIMIT_SETTING.get(s));
    }

    public void testRootLimitSettingExplicitOverridesDerived() {
        Settings s = Settings.builder()
            .put("node.native_memory.limit", "8gb")
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 1024L)
            .build();
        assertEquals(Long.valueOf(1024L), ArrowBasePlugin.ROOT_LIMIT_SETTING.get(s));
    }

    public void testRootLimitRejectsNegative() {
        Settings s = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, -1L).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ArrowBasePlugin.ROOT_LIMIT_SETTING.get(s));
        assertTrue(e.getMessage().contains("must be >= 0"));
    }

    public void testQuerySettingsExposeDefaults() {
        Settings s = Settings.EMPTY;
        assertEquals(Long.valueOf(0L), ArrowBasePlugin.QUERY_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testFlightAndIngestMinDefaultsToZero() {
        // The grouped validator (validateMinSum) treats per-pool mins as a guarantee
        // floor — defaults of Long.MAX_VALUE caused the validator to reject any PUT
        // that set a non-MAX root. Pool mins must default to zero so the baseline
        // configuration is consistent.
        Settings s = Settings.EMPTY;
        assertEquals(Long.valueOf(0L), ArrowBasePlugin.FLIGHT_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(0L), ArrowBasePlugin.INGEST_MIN_SETTING.get(s));
    }

    public void testQuerySettingsAcceptValues() {
        Settings s = Settings.builder()
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MIN, 100L)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 1000L)
            .build();
        assertEquals(Long.valueOf(100L), ArrowBasePlugin.QUERY_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(1000L), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    // -----------------------------------------------------------------
    // End-to-end wiring tests — verify that Setting.Property.Dynamic settings
    // actually flow through to the live allocator. These guard against the
    // "dynamic in name only" failure mode where a setting parses, the validator
    // runs, the cluster-state update succeeds, and the runtime component
    // silently does nothing because the addSettingsUpdateConsumer line was
    // never registered. Bare-setter unit tests do not catch this; tests must
    // drive a real ClusterSettings#applySettings round-trip.
    // -----------------------------------------------------------------

    /**
     * Builds a {@link ClusterSettings} preloaded with all of {@link ArrowBasePlugin}'s
     * settings, mirroring what {@code SettingsModule} does at node startup. Returns the
     * fresh allocator with the framework's pools created and consumers registered
     * — the same wiring path {@code createComponents} runs.
     */
    private static ArrowNativeAllocator newWiredAllocator(Settings nodeSettings, ClusterSettings cs) {
        long rootLimit = ArrowBasePlugin.ROOT_LIMIT_SETTING.get(nodeSettings);
        ArrowNativeAllocator allocator = new ArrowNativeAllocator(rootLimit);
        allocator.setRebalanceInterval(ArrowBasePlugin.REBALANCE_INTERVAL_SETTING.get(nodeSettings));
        allocator.getOrCreatePool(
            NativeAllocatorPoolConfig.POOL_FLIGHT,
            ArrowBasePlugin.FLIGHT_MIN_SETTING.get(nodeSettings),
            ArrowBasePlugin.FLIGHT_MAX_SETTING.get(nodeSettings)
        );
        allocator.getOrCreatePool(
            NativeAllocatorPoolConfig.POOL_INGEST,
            ArrowBasePlugin.INGEST_MIN_SETTING.get(nodeSettings),
            ArrowBasePlugin.INGEST_MAX_SETTING.get(nodeSettings)
        );
        allocator.getOrCreatePool(
            NativeAllocatorPoolConfig.POOL_QUERY,
            ArrowBasePlugin.QUERY_MIN_SETTING.get(nodeSettings),
            ArrowBasePlugin.QUERY_MAX_SETTING.get(nodeSettings)
        );
        ArrowBasePlugin.registerSettingsUpdateConsumers(cs, allocator);
        return allocator;
    }

    private static ClusterSettings newClusterSettings(Settings nodeSettings) {
        Set<Setting<?>> registered = new HashSet<>();
        registered.addAll(new ArrowBasePlugin().getSettings());
        return new ClusterSettings(nodeSettings, registered);
    }

    public void testQueryMaxClusterSettingPropagatesToAllocator() {
        // The full wired path: node starts at default settings, plugin registers
        // consumers, operator PUTs a new max via _cluster/settings.
        Settings nodeSettings = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024).build();
        ClusterSettings cs = newClusterSettings(nodeSettings);
        ArrowNativeAllocator allocator = newWiredAllocator(nodeSettings, cs);
        try {
            cs.applySettings(
                Settings.builder()
                    .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024)
                    .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 1024L * 1024 * 1024)
                    .build()
            );
            assertEquals(
                "PUT to query max must update the live BufferAllocator limit",
                1024L * 1024 * 1024,
                allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY).getLimit()
            );
            assertEquals(1024L * 1024 * 1024, allocator.getPoolMax(NativeAllocatorPoolConfig.POOL_QUERY));
        } finally {
            allocator.close();
        }
    }

    public void testFlightMinClusterSettingPropagatesToAllocator() {
        // Min is the regression-prone path: prior to the live-propagation fix,
        // setPoolMin only updated the poolMins map and operators got HTTP 200 with
        // no observable behavior change.
        Settings nodeSettings = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024).build();
        ClusterSettings cs = newClusterSettings(nodeSettings);
        ArrowNativeAllocator allocator = newWiredAllocator(nodeSettings, cs);
        try {
            // Pool starts at max (rebalancer disabled by default), so a min PUT below
            // the current limit is a no-op on the live limit but updates poolMins.
            // Use a min ABOVE the current limit to force the live raise path.
            cs.applySettings(
                Settings.builder()
                    .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024)
                    .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, 4L * 1024 * 1024 * 1024)
                    .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN, 2L * 1024 * 1024 * 1024)
                    .build()
            );
            assertEquals(
                "PUT to flight min must update the recorded min for the rebalancer",
                2L * 1024 * 1024 * 1024,
                allocator.getPoolMin(NativeAllocatorPoolConfig.POOL_FLIGHT)
            );
            assertTrue(
                "PUT to flight min must raise the live BufferAllocator limit when min exceeds current",
                allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT).getLimit() >= 2L * 1024 * 1024 * 1024
            );
        } finally {
            allocator.close();
        }
    }

    public void testRootLimitClusterSettingPropagatesToAllocator() {
        Settings nodeSettings = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024).build();
        ClusterSettings cs = newClusterSettings(nodeSettings);
        ArrowNativeAllocator allocator = newWiredAllocator(nodeSettings, cs);
        try {
            cs.applySettings(
                Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 16L * 1024 * 1024 * 1024).build()
            );
            assertEquals(
                "PUT to root limit must update the RootAllocator's limit",
                16L * 1024 * 1024 * 1024,
                allocator.getRootAllocator().getLimit()
            );
        } finally {
            allocator.close();
        }
    }

    public void testValidatorRejectsSumOfMinsExceedingRoot() {
        // The cross-setting grouped validator must reject PUTs that would over-
        // subscribe the root. Test the rejection path end-to-end through ClusterSettings.
        Settings nodeSettings = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 1024L).build();
        ClusterSettings cs = newClusterSettings(nodeSettings);
        ArrowNativeAllocator allocator = newWiredAllocator(nodeSettings, cs);
        try {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> cs.applySettings(
                    Settings.builder()
                        .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 1024L)
                        .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN, 600L)
                        .put(NativeAllocatorPoolConfig.SETTING_INGEST_MIN, 600L)
                        .build()
                )
            );
            assertTrue(
                "expected sum-exceeds-root in error, got: " + e.getMessage(),
                e.getMessage().contains("exceeds root limit") || e.getMessage().contains("Sum of pool minimums")
            );
        } finally {
            allocator.close();
        }
    }

    public void testListenerFiresAndChildAllocatorTracksUpdate() {
        // Sanity check for the AnalyticsSearchService / FlightTransport pattern:
        // when a consumer creates a child of the framework's pool and registers a
        // listener, a PUT to the pool's max propagates into the child's setLimit.
        Settings nodeSettings = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024).build();
        ClusterSettings cs = newClusterSettings(nodeSettings);
        ArrowNativeAllocator allocator = newWiredAllocator(nodeSettings, cs);
        try {
            BufferAllocator queryPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY);
            BufferAllocator child = queryPool.newChildAllocator("consumer", 0, queryPool.getLimit());
            try {
                allocator.addListener((poolName, newLimit) -> {
                    if (NativeAllocatorPoolConfig.POOL_QUERY.equals(poolName)) {
                        child.setLimit(newLimit);
                    }
                });
                cs.applySettings(
                    Settings.builder()
                        .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024)
                        .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 2L * 1024 * 1024 * 1024)
                        .build()
                );
                assertEquals(
                    "child allocator must mirror its parent pool's new limit via the listener",
                    2L * 1024 * 1024 * 1024,
                    child.getLimit()
                );
            } finally {
                child.close();
            }
        } finally {
            allocator.close();
        }
    }
}
