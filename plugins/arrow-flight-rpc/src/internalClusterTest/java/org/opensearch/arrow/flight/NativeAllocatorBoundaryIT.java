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
 * configured caps are enforced at allocation time.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 1, maxNumDataNodes = 1)
public class NativeAllocatorBoundaryIT extends OpenSearchIntegTestCase {

    /** 1 MiB. */
    private static final long MB = 1024L * 1024;

    /** Per-pool cap for tests. */
    private static final long POOL_CAP_BYTES = 16 * MB;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("node.native_memory.limit", "256mb")
            .put("native.allocator.rebalancer.enabled", false)
            // Per-pool maxes set to a known value for testing.
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, POOL_CAP_BYTES)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, POOL_CAP_BYTES)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, POOL_CAP_BYTES)
            .build();
    }

    /**
     * Verifies that per-pool max caps allocations through the QUERY pool.
     */
    public void testPoolMaxRejectsAllocationsBeyondCap() {
        ArrowNativeAllocator allocator = internalCluster().getInstance(ArrowNativeAllocator.class);
        BufferAllocator queryPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY);
        assertThat("pool's live limit reflects the configured max", queryPool.getLimit(), is(POOL_CAP_BYTES));

        // Sub-cap allocation succeeds.
        try (var withinCap = queryPool.buffer(2 * MB)) {
            assertThat(queryPool.getAllocatedMemory(), greaterThanOrEqualTo(2 * MB));
        }

        // Cap+1 allocation fails.
        expectThrows(OutOfMemoryException.class, () -> queryPool.buffer(POOL_CAP_BYTES + MB));
    }

    /**
     * Verifies that per-pool limits cap allocations: when one pool is full,
     * allocations through it fail even if other pools have headroom.
     */
    public void testPoolLimitRejectsAllocationsBeyondCap() {
        ArrowNativeAllocator allocator = internalCluster().getInstance(ArrowNativeAllocator.class);
        BufferAllocator flightPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT);
        BufferAllocator queryPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY);

        assertThat("flight.max must match nodeSettings", flightPool.getLimit(), is(POOL_CAP_BYTES));
        assertThat("query.max must match nodeSettings", queryPool.getLimit(), is(POOL_CAP_BYTES));

        // Hold 8 MiB through the FLIGHT pool.
        try (var flightHold = flightPool.buffer(8 * MB)) {
            assertThat("FLIGHT pool reflects 8MB allocation", flightPool.getAllocatedMemory(), is(8L * MB));

            // A 4 MiB allocation through QUERY succeeds (within its own pool cap).
            try (var queryFit = queryPool.buffer(4 * MB)) {
                assertThat(queryPool.getAllocatedMemory(), greaterThanOrEqualTo(4 * MB));
            }

            // An allocation exceeding the QUERY pool's own cap fails.
            expectThrows(OutOfMemoryException.class, () -> queryPool.buffer(POOL_CAP_BYTES + MB));
        }
    }

    /**
     * Verifies that setPoolLimit dynamically adjusts the pool cap and
     * subsequent allocations respect the new limit.
     */
    public void testSetPoolLimitAffectsInFlightAllocations() {
        ArrowNativeAllocator allocator = internalCluster().getInstance(ArrowNativeAllocator.class);
        BufferAllocator queryPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY);

        try (BufferAllocator child = queryPool.newChildAllocator("boundary-it-child", 0, Long.MAX_VALUE)) {
            // A small buffer through the child succeeds with the initial pool max.
            try (var buf = child.buffer(2 * MB)) {
                assertThat(child.getAllocatedMemory(), greaterThanOrEqualTo(2 * MB));
            }

            // Programmatically tighten the pool limit.
            long newPoolCap = 1 * MB;
            allocator.setPoolLimit(NativeAllocatorPoolConfig.POOL_QUERY, newPoolCap);
            assertThat("pool's own limit reflects the update", queryPool.getLimit(), is(newPoolCap));

            // An allocation that fit before the resize now exceeds the parent cap.
            expectThrows(OutOfMemoryException.class, () -> child.buffer(2 * MB));

            // An allocation under the new cap still succeeds.
            try (var smallBuf = child.buffer(512 * 1024)) {
                assertThat(child.getAllocatedMemory(), greaterThanOrEqualTo(512L * 1024));
            }
        }
    }
}
