/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

public class ArrowBasePluginTests extends OpenSearchTestCase {

    public void testDeriveRootLimitDefaultUnsetReturnsLongMaxValue() {
        // Explicit 0 expresses "AC unconfigured" — default is now ram - heap, so Settings.EMPTY
        // would resolve to a real value on whatever machine the test runs on.
        Settings s = Settings.builder().put("node.native_memory.limit", "0b").build();
        assertEquals(Long.toString(Long.MAX_VALUE), ArrowBasePlugin.deriveRootLimitDefault(s));
    }

    public void testDeriveRootLimitDefaultUsesAcLimitWhenSet() {
        Settings s = Settings.builder().put("node.native_memory.limit", "1gb").build();
        // ROOT_LIMIT defaults to 20% of node.native_memory.limit — the Arrow framework gets a
        // small fraction of native budget; DataFusion's Rust runtime takes the larger share.
        long oneGiB = 1024L * 1024 * 1024;
        assertEquals(Long.toString(oneGiB * 20 / 100), ArrowBasePlugin.deriveRootLimitDefault(s));
    }

    public void testDeriveRootLimitDefaultIgnoresBufferPercent() {
        // node.native_memory.buffer_percent is admission control's throttle margin, not a
        // framework budget reduction. The framework default takes its 20% fraction off
        // node.native_memory.limit directly so AC's safety margin sits between AC's throttle
        // threshold and the framework's hard cap rather than being collapsed into the cap.
        // 1000 bytes limit, 20% buffer => root.limit still 20% of 1000 = 200.
        Settings s = Settings.builder().put("node.native_memory.limit", "1000b").put("node.native_memory.buffer_percent", 20).build();
        assertEquals("200", ArrowBasePlugin.deriveRootLimitDefault(s));
    }

    public void testRootLimitSettingExposesDerivedDefault() {
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").build();
        // 20% of 10 GiB.
        long expected = 10L * 1024 * 1024 * 1024 * 20 / 100;
        assertEquals(Long.valueOf(expected), ArrowBasePlugin.ROOT_LIMIT_SETTING.get(s));
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
        // Explicit 0 expresses "AC unconfigured" so QUERY_MAX falls back to Long.MAX_VALUE.
        // Settings.EMPTY would resolve via ram - heap default to a finite, machine-dependent value.
        Settings s = Settings.builder().put("node.native_memory.limit", "0b").build();
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

    // -- Pool max defaults derived from node.native_memory.limit ----------
    // Pool maxes anchor to the operator's off-heap budget (node.native_memory.limit),
    // not to native.allocator.root.limit. This matches the PR #21732 partitioning
    // diagram where pool fractions (5%/8%/5%) are of native_memory.limit. Sum of
    // pool maxes (18% of native_memory.limit) fits within root.limit (20% of
    // native_memory.limit) by default, leaving 2 pp headroom inside the root cap.

    public void testPoolMaxDefaultsAreLongMaxValueWhenAcUnset() {
        // AC explicitly unconfigured — pool maxes default to Long.MAX_VALUE (unbounded),
        // preserving pre-AC behaviour. The default for node.native_memory.limit is
        // 79% of (ram - heap), so to test the "unset" branch we must explicitly set it to 0.
        Settings s = Settings.builder().put("node.native_memory.limit", "0b").build();
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.FLIGHT_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.INGEST_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testPoolMaxDefaultsScaleFromAcBudget() {
        // 10 GiB native memory limit. Pool maxes per the partitioning model in PR #21732:
        // FLIGHT_MAX = 5% INGEST_MAX = 8% QUERY_MAX = 5%
        // Anchored to node.native_memory.limit, not to root.limit (which defaults to 20%
        // of native_memory.limit) — see derivePoolMaxDefault Javadoc.
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").build();
        long limit = 10L * 1024 * 1024 * 1024;
        assertEquals(Long.valueOf(limit * 5 / 100), ArrowBasePlugin.FLIGHT_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(limit * 8 / 100), ArrowBasePlugin.INGEST_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(limit * 5 / 100), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testPoolMaxDefaultsIgnoreRootLimitOverride() {
        // Pool maxes anchor to node.native_memory.limit, not to root.limit. An operator
        // who overrides root.limit (e.g. to 4 GiB instead of the default 20% of
        // native_memory.limit = 2 GiB) does not shrink pool defaults proportionally;
        // the diagrammed partitioning of native_memory.limit holds.
        Settings s = Settings.builder()
            .put("node.native_memory.limit", "10gb")
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 4L * 1024 * 1024 * 1024)
            .build();
        long limit = 10L * 1024 * 1024 * 1024;
        assertEquals(Long.valueOf(limit * 5 / 100), ArrowBasePlugin.FLIGHT_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(limit * 8 / 100), ArrowBasePlugin.INGEST_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(limit * 5 / 100), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testPoolMaxDefaultsIgnoreBufferPercent() {
        // node.native_memory.buffer_percent is AC's throttle margin, not a framework budget
        // reduction. Pool maxes default off node.native_memory.limit directly so AC's safety
        // margin sits between AC's throttle threshold and the framework's hard cap rather than
        // being collapsed into the cap.
        // 1000 bytes limit, 20% buffer => pool maxes are still 5/8/5% of 1000 = 50/80/50.
        Settings s = Settings.builder().put("node.native_memory.limit", "1000b").put("node.native_memory.buffer_percent", 20).build();
        assertEquals(Long.valueOf(50L), ArrowBasePlugin.FLIGHT_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(80L), ArrowBasePlugin.INGEST_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(50L), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testPoolMaxExplicitOverridesDerived() {
        // Operator-set values must win over derived defaults.
        Settings s = Settings.builder()
            .put("node.native_memory.limit", "10gb")
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, 7L)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, 8L)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 9L)
            .build();
        assertEquals(Long.valueOf(7L), ArrowBasePlugin.FLIGHT_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(8L), ArrowBasePlugin.INGEST_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(9L), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
    }

    public void testPoolMaxRejectsNegative() {
        // Negative pool max is rejected at parse time, mirroring ROOT_LIMIT_SETTING.
        // Each pool's parser has its own message so we exercise all three to lock down
        // the per-pool error contract (and keep coverage honest on what is otherwise
        // boilerplate-but-distinct branches).
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

    public void testBuildAllocatorWiresAllPoolsAndSettingsConsumers() {
        // Verifies the full createComponents code path — the helper extracted from
        // createComponents builds the allocator, creates all three pools (FLIGHT, INGEST,
        // QUERY), and registers the cluster-settings update consumers. We bypass the
        // heavyweight ClusterService fixture and inject a real ClusterSettings directly,
        // which is what production wiring also passes through to buildAllocator after
        // unpacking the createComponents arguments.
        Settings nodeSettings = Settings.builder()
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, 1L * 1024 * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, 2L * 1024 * 1024 * 1024)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 1L * 1024 * 1024 * 1024)
            .build();
        ClusterSettings cs = newClusterSettings(nodeSettings);

        ArrowNativeAllocator allocator = ArrowBasePlugin.buildAllocator(nodeSettings, cs);
        try {
            // All three pools created.
            Set<String> poolNames = allocator.getPoolNames();
            assertEquals("buildAllocator must register exactly the framework's three pools", 3, poolNames.size());
            assertTrue(poolNames.contains(NativeAllocatorPoolConfig.POOL_FLIGHT));
            assertTrue(poolNames.contains(NativeAllocatorPoolConfig.POOL_INGEST));
            assertTrue(poolNames.contains(NativeAllocatorPoolConfig.POOL_QUERY));

            // Pool maxes match the operator-set values (rebalancer disabled by default,
            // so initial limit == max).
            assertEquals(1L * 1024 * 1024 * 1024, allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT).getLimit());
            assertEquals(2L * 1024 * 1024 * 1024, allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST).getLimit());
            assertEquals(1L * 1024 * 1024 * 1024, allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY).getLimit());

            // Cluster-settings update consumers are registered: a PUT to a pool max must
            // propagate to the live allocator.
            cs.applySettings(
                Settings.builder()
                    .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024)
                    .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, 4L * 1024 * 1024 * 1024)
                    .build()
            );
            assertEquals(
                "buildAllocator must wire the INGEST_MAX cluster-settings consumer",
                4L * 1024 * 1024 * 1024,
                allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST).getLimit()
            );
        } finally {
            allocator.close();
        }
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
            cs.applySettings(Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 16L * 1024 * 1024 * 1024).build());
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
        // Set node.native_memory.limit=0b explicitly so pool maxes default to Long.MAX_VALUE
        // — min<max is trivially satisfied and the validator catches sum-exceeds-root
        // cleanly without the per-pool min>max path firing first.
        Settings nodeSettings = Settings.builder()
            .put("node.native_memory.limit", "0b")
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 10L * 1024 * 1024 * 1024)
            .build();
        ClusterSettings cs = newClusterSettings(nodeSettings);
        ArrowNativeAllocator allocator = newWiredAllocator(nodeSettings, cs);
        try {
            // root=10gb, flight_min=6gb, ingest_min=6gb => sum_mins=12gb > root=10gb.
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> cs.applySettings(
                    Settings.builder()
                        .put("node.native_memory.limit", "0b")
                        .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 10L * 1024 * 1024 * 1024)
                        .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN, 6L * 1024 * 1024 * 1024)
                        .put(NativeAllocatorPoolConfig.SETTING_INGEST_MIN, 6L * 1024 * 1024 * 1024)
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

    public void testChildAllocatorInheritsParentCapAfterPoolLimitUpdate() {
        // Sanity check for the AnalyticsSearchService / FlightTransport pattern:
        // when a consumer creates a child of the framework's pool with Long.MAX_VALUE
        // limit, a PUT to the pool's max takes effect on the child's allocations
        // automatically via Arrow's parent-cap check at allocateBytes — no listener needed.
        //
        // The contract we rely on (Arrow Accountant.allocate, lines 191-203 in 18.3.0):
        // when the child's reservation is exhausted, it calls parent.allocate(...) which
        // checks the parent's allocationLimit on every allocation. Setting the child's own
        // limit to Long.MAX_VALUE means the child has no own-cap on top of the parent's;
        // setLimit on the parent is observed atomically by all subsequent allocations
        // through any descendant.
        Settings nodeSettings = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024).build();
        ClusterSettings cs = newClusterSettings(nodeSettings);
        ArrowNativeAllocator allocator = newWiredAllocator(nodeSettings, cs);
        try {
            BufferAllocator queryPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY);
            BufferAllocator child = queryPool.newChildAllocator("consumer", 0, Long.MAX_VALUE);
            try {
                // Step 1: a small allocation through the child succeeds with the original pool max.
                try (var buf = child.buffer(1024)) {
                    assertEquals("child accounting reflects allocation", 1024L, child.getAllocatedMemory());
                    assertEquals("parent pool sees child allocation", 1024L, queryPool.getAllocatedMemory());
                }

                // Step 2: PUT a small pool max via cluster settings.
                cs.applySettings(
                    Settings.builder()
                        .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024)
                        .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 1L * 1024 * 1024)  // 1 MB
                        .build()
                );
                assertEquals("pool's own limit reflects the PUT", 1L * 1024 * 1024, queryPool.getLimit());
                assertEquals("child's own limit is intentionally uncapped", Long.MAX_VALUE, child.getLimit());

                // Step 3: allocations within the new parent cap still work.
                try (var withinCap = child.buffer(512 * 1024)) {  // 512 KB, under 1 MB cap
                    assertEquals(512L * 1024, child.getAllocatedMemory());
                }

                // Step 4: allocation exceeding the new parent cap fails — this is the
                // behavior the deleted listener pattern was emulating, now provided
                // natively by Arrow's parent-cap check.
                expectThrows(OutOfMemoryException.class, () -> child.buffer(2L * 1024 * 1024));  // 2 MB, over 1 MB cap
            } finally {
                child.close();
            }
        } finally {
            allocator.close();
        }
    }
}
