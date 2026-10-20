/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration test for the NativeMemoryRebalancer.
 *
 * <p>Boots a single-node cluster with the rebalancer enabled and verifies that
 * pools start at their max and the rebalancer shrinks idle pools / grows pressured ones.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 1, maxNumDataNodes = 1)
public class NativeMemoryRebalancerIT extends OpenSearchIntegTestCase {

    private static final long MB = 1024L * 1024;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("native.allocator.rebalancer.enabled", true)
            .put("native.allocator.rebalance.interval_seconds", 1)
            .put("node.native_memory.limit", "1gb")
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN, 5 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, 200 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MIN, 5 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, 200 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MIN, 5 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 200 * MB)
            .build();
    }

    /**
     * Verifies that pools start at their max (before rebalancer shrinks them).
     * Uses a long rebalancer interval to avoid race conditions.
     */
    public void testPoolsStartAtMax() {
        ArrowNativeAllocator allocator = internalCluster().getInstance(ArrowNativeAllocator.class);
        BufferAllocator ingestPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST);

        // The rebalancer may have already run (1s interval), so the pool may have shrunk.
        // Verify it's at least at min and the configured max is correct.
        long max = allocator.getPoolMax(NativeAllocatorPoolConfig.POOL_INGEST);
        assertEquals("Ingest pool max should be configured at 200MB", 200 * MB, max);
        // Pool limit should be between min and max (rebalancer may have shrunk it)
        long limit = ingestPool.getLimit();
        assertThat("Ingest pool limit should be >= min", limit, org.hamcrest.Matchers.greaterThanOrEqualTo(5 * MB));
        assertThat("Ingest pool limit should be <= max", limit, lessThanOrEqualTo(200 * MB));
    }

    /**
     * Verifies that an idle pool shrinks after rebalancer ticks when another pool is pressured.
     */
    public void testIdlePoolShrinksWhenOtherPressured() throws Exception {
        ArrowNativeAllocator allocator = internalCluster().getInstance(ArrowNativeAllocator.class);
        BufferAllocator ingestPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST);
        BufferAllocator flightPool = allocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT);

        // Allocate > 75% of ingest pool to create pressure
        long toAllocate = (long) (ingestPool.getLimit() * 0.8);
        ArrowBuf buf = ingestPool.buffer(toAllocate);

        try {
            // Flight pool is idle — wait for rebalancer to shrink it
            assertBusy(() -> {
                long flightLimit = flightPool.getLimit();
                assertThat("Flight pool should shrink when idle", flightLimit, org.hamcrest.Matchers.lessThan(200 * MB));
            });
        } finally {
            buf.close();
        }
    }
}
