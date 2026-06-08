/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the native-allocator framework's cap enforcement.
 *
 * <p>Boots a single-node cluster with tight memory settings, then exercises
 * the actual Arrow allocation path to verify that the framework's
 * configured caps are enforced at allocation time (not just at config-parse
 * time). Complements unit-level tests in {@code ArrowBasePluginTests} by
 * verifying that the production wiring (Guice -> ArrowNativeAllocator ->
 * Arrow's RootAllocator chain) honors the caps end-to-end.
 *
 * <p>Each test sets explicit byte limits and allocates real
 * {@link org.apache.arrow.memory.ArrowBuf} buffers, asserting either
 * successful allocation or {@link OutOfMemoryException} based on whether
 * the request fits within the configured cap.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 1, maxNumDataNodes = 1)
public class NativeAllocatorBoundaryIT extends OpenSearchIntegTestCase {

    /** 1 MiB. Chosen small enough that tests run fast but large enough that
     *  Arrow's internal accounting doesn't round it away. */
    private static final long MB = 1024L * 1024;

    /** Cap large enough for the framework's own bookkeeping but small enough
     *  to trigger OOM well before exhausting host memory. */
    private static final long ROOT_CAP_BYTES = 16 * MB;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // Set node.native_memory.limit explicitly so framework defaults derive
        // from a known value rather than the (machine-dependent) ram-heap default.
        // ROOT_LIMIT and pool maxes are then overridden per-test via cluster
        // settings PUT or directly via this node-settings layer.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("node.native_memory.limit", "256mb")
            // Tight root cap: 16 MiB total Arrow framework budget.
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, ROOT_CAP_BYTES)
            // Per-pool maxes set generously so per-pool caps don't trip
            // before root.limit. Tests targeting per-pool caps override below.
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, ROOT_CAP_BYTES)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, ROOT_CAP_BYTES)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, ROOT_CAP_BYTES)
            .build();
    }

    /**
     * Verifies that {@code parquet.native.pool.query.max} caps allocations
     * through the QUERY pool: a buffer request exceeding the per-pool cap
     * throws {@link OutOfMemoryException} even when root has headroom.
     */
    public void testPoolMaxRejectsAllocationsBeyondCap() {
        // Tighten QUERY pool to 4 MiB while leaving root at 16 MiB.
        long poolCap = 4 * MB;
        ClusterUpdateSettingsResponse resp = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, poolCap))
            .get();
        assertTrue("PUT to query.max must succeed", resp.isAcknowledged());

        ArrowNativeAllocator allocator = internalCluster().getInstance(ArrowNativeAllocator.class);
        BufferAllocator queryPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY);
        assertThat("pool's live limit reflects the PUT", queryPool.getLimit(), is(poolCap));

        // Sub-cap allocation succeeds.
        try (var withinCap = queryPool.buffer(2 * MB)) {
            assertThat(queryPool.getAllocatedMemory(), greaterThanOrEqualTo(2 * MB));
        }

        // Cap+1 allocation fails — Arrow's parent-cap check at allocateBytes
        // walks queryPool's allocationLimit and rejects.
        expectThrows(OutOfMemoryException.class, () -> queryPool.buffer(8 * MB));
    }

    /**
     * Verifies that {@code native.allocator.root.limit} caps allocations
     * across all pools combined: when the sum of in-flight pool allocations
     * approaches the root cap, the next allocation is rejected at the root
     * level even if each individual pool's max would allow it.
     */
    public void testRootLimitRejectsAllocationsBeyondCap() {
        ArrowNativeAllocator allocator = internalCluster().getInstance(ArrowNativeAllocator.class);
        BufferAllocator flightPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT);
        BufferAllocator queryPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY);
        BufferAllocator root = allocator.getRootAllocator();

        // Sanity-check setup: confirm the live limits match nodeSettings.
        // If these fail, the test setup is wrong and the body's expectations are
        // meaningless — surface the misconfiguration instead of misleading failures below.
        assertThat("root.limit must match nodeSettings", root.getLimit(), is(ROOT_CAP_BYTES));
        assertThat("flight.max must match nodeSettings", flightPool.getLimit(), is(ROOT_CAP_BYTES));
        assertThat("query.max must match nodeSettings", queryPool.getLimit(), is(ROOT_CAP_BYTES));

        // Hold 8 MiB through the FLIGHT pool. With root at 16 MiB this leaves 8 MiB
        // headroom across the root. (Power-of-2 sizes avoid Arrow's chunked-allocation
        // rounding surprises; e.g. a 12 MiB request actually consumes 16 MiB.)
        try (var flightHold = flightPool.buffer(8 * MB)) {
            assertThat("FLIGHT pool reflects 8MB allocation", flightPool.getAllocatedMemory(), is(8L * MB));
            assertThat("root reflects 8MB allocation", root.getAllocatedMemory(), is(8L * MB));

            // A 4 MiB allocation through QUERY succeeds (within remaining root headroom).
            try (var queryFit = queryPool.buffer(4 * MB)) {
                assertThat(allocator.getRootAllocator().getAllocatedMemory(), is(12L * MB));
            }

            // An 8 MiB allocation through QUERY would push the root past the 16 MiB cap
            // (8 MiB FLIGHT + 8 MiB QUERY). Arrow's parent-cap check at allocateBytes
            // walks queryPool -> root and rejects with OOM, even though QUERY's own
            // (16 MiB) max would individually allow it.
            expectThrows(OutOfMemoryException.class, () -> queryPool.buffer(16 * MB));
        }
    }

    /**
     * Verifies that a dynamic PUT to a pool's max takes effect on
     * subsequent allocations through descendants of that pool. This is the
     * behavior the deleted {@code NativeAllocatorListener} SPI was emulating;
     * it is now provided natively by Arrow's parent-cap check at allocateBytes.
     */
    public void testDynamicPoolResizeAffectsInFlightAllocations() {
        ArrowNativeAllocator allocator = internalCluster().getInstance(ArrowNativeAllocator.class);
        BufferAllocator queryPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY);

        // Step 1: create a child allocator at Long.MAX_VALUE — the AnalyticsSearchService /
        // DefaultPlanExecutor pattern. The child intentionally has no own-cap; it relies
        // on the parent pool's allocationLimit at allocation time.
        try (BufferAllocator child = queryPool.newChildAllocator("boundary-it-child", 0, Long.MAX_VALUE)) {
            // Step 2: a small buffer through the child succeeds with the initial pool max.
            try (var buf = child.buffer(2 * MB)) {
                assertThat(child.getAllocatedMemory(), greaterThanOrEqualTo(2 * MB));
            }

            // Step 3: PUT a tighter pool max via cluster settings.
            long newPoolCap = 1 * MB;
            ClusterUpdateSettingsResponse resp = client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, newPoolCap))
                .get();
            assertTrue("PUT to query.max must succeed", resp.isAcknowledged());
            assertThat("pool's own limit reflects the PUT", queryPool.getLimit(), is(newPoolCap));
            assertThat("child's own limit is intentionally uncapped", child.getLimit(), is(Long.MAX_VALUE));

            // Step 4: an allocation that fit before the resize now exceeds the parent cap.
            // Arrow's parent-cap check at allocateBytes walks queryPool.allocationLimit
            // and rejects — no listener machinery needed.
            expectThrows(OutOfMemoryException.class, () -> child.buffer(2 * MB));

            // Step 5: an allocation under the new cap still succeeds.
            try (var smallBuf = child.buffer(512 * 1024)) {
                assertThat(child.getAllocatedMemory(), greaterThanOrEqualTo(512L * 1024));
            }
        }
    }
}
