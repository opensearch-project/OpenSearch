/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.spi.NativeAllocator;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowNativeAllocatorTests extends OpenSearchTestCase {

    private ArrowNativeAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new ArrowNativeAllocator(1024L * 1024 * 1024); // 1 GB for tests
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testCreatePool() {
        NativeAllocator.PoolHandle handle = allocator.getOrCreatePool("test-pool", 100 * 1024 * 1024);
        assertNotNull(handle);
        assertEquals(100 * 1024 * 1024, handle.limit());
        assertEquals(0, handle.allocatedBytes());
    }

    public void testGetOrCreatePoolIdempotent() {
        NativeAllocator.PoolHandle first = allocator.getOrCreatePool("idempotent", 50 * 1024 * 1024);
        NativeAllocator.PoolHandle second = allocator.getOrCreatePool("idempotent", 999 * 1024 * 1024);
        assertSame(first, second);
        assertEquals(50 * 1024 * 1024, second.limit());
    }

    public void testPoolChildAllocation() {
        allocator.getOrCreatePool("parent", 200 * 1024 * 1024);
        BufferAllocator child = allocator.getPoolAllocator("parent").newChildAllocator("child-1", 0, 50 * 1024 * 1024);
        try {
            child.buffer(1024).close();
            assertTrue(allocator.getPoolAllocator("parent").getAllocatedMemory() == 0);
        } finally {
            child.close();
        }
    }

    public void testSetPoolLimit() {
        allocator.getOrCreatePool("resizable", 100 * 1024 * 1024);
        allocator.setPoolLimit("resizable", 200 * 1024 * 1024);
        assertEquals(200 * 1024 * 1024, allocator.getPoolAllocator("resizable").getLimit());
    }

    public void testSetPoolLimitNonExistent() {
        expectThrows(IllegalStateException.class, () -> allocator.setPoolLimit("ghost", 100));
    }

    public void testGetPoolAllocatorNonExistent() {
        expectThrows(IllegalStateException.class, () -> allocator.getPoolAllocator("ghost"));
    }

    public void testSetRootLimit() {
        allocator.setRootLimit(512 * 1024 * 1024);
        assertEquals(512 * 1024 * 1024, allocator.getRootAllocator().getLimit());
    }

    public void testStats() {
        allocator.getOrCreatePool("stats-pool", 64 * 1024 * 1024);
        NativeAllocatorPoolStats stats = allocator.stats();

        assertNotNull(stats);
        assertEquals(1024 * 1024 * 1024, stats.getRootLimitBytes());
        assertEquals(0, stats.getRootAllocatedBytes());
        assertEquals(1, stats.getPools().size());

        NativeAllocatorPoolStats.PoolStats poolStats = stats.getPools().get(0);
        assertEquals("stats-pool", poolStats.getName());
        assertEquals(64 * 1024 * 1024, poolStats.getLimitBytes());
        assertEquals(0, poolStats.getAllocatedBytes());
        // child_count is no longer rendered in stats; getPoolAllocator(...).getChildAllocators()
        // is the runtime accessor for that detail if needed.
    }

    public void testStatsMultiplePools() {
        allocator.getOrCreatePool("pool-a", 100 * 1024 * 1024);
        allocator.getOrCreatePool("pool-b", 200 * 1024 * 1024);

        NativeAllocatorPoolStats stats = allocator.stats();
        assertEquals(2, stats.getPools().size());
    }

    public void testGetPoolNames() {
        allocator.getOrCreatePool("alpha", 10 * 1024 * 1024);
        allocator.getOrCreatePool("beta", 20 * 1024 * 1024);

        assertTrue(allocator.getPoolNames().contains("alpha"));
        assertTrue(allocator.getPoolNames().contains("beta"));
        assertEquals(2, allocator.getPoolNames().size());
    }

    public void testRebalanceDistributesHeadroomToAllPools() {
        allocator.setRootLimit(100 * 1024 * 1024);
        allocator.getOrCreatePool("active", 10 * 1024 * 1024, 100 * 1024 * 1024);
        allocator.getOrCreatePool("idle", 10 * 1024 * 1024, 100 * 1024 * 1024);

        // Simulate activity: allocate in "active" pool.
        BufferAllocator activeAlloc = allocator.getPoolAllocator("active");
        BufferAllocator child = activeAlloc.newChildAllocator("worker", 0, 100 * 1024 * 1024);
        var buf = child.buffer(5 * 1024 * 1024);

        try {
            allocator.rebalance();

            // Active pool gets bonus headroom on top of its min.
            long activeLimit = activeAlloc.getLimit();
            assertTrue("Active pool limit should exceed min after rebalance, got " + activeLimit, activeLimit > 10 * 1024 * 1024);

            // Idle pool also receives headroom: distributing to all pools (not just
            // currently-active ones) avoids the dead-pool corner case where a pool
            // with min = 0 starts at limit = 0 and can never make a first allocation.
            // Idle pools that don't end up needing the headroom return it on the next
            // tick once they remain at zero allocation.
            long idleLimit = allocator.getPoolAllocator("idle").getLimit();
            assertTrue("Idle pool should also receive headroom, got " + idleLimit, idleLimit > 10 * 1024 * 1024);
        } finally {
            buf.close();
            child.close();
        }
    }

    public void testRebalanceLetsZeroMinPoolAllocate() {
        // Regression test: under the previous "active pools only" rebalance algorithm,
        // a pool with min = 0 would start at limit = 0 (rebalancer-on path), be unable
        // to allocate, never become "active", and so never receive a bonus — permanently
        // dead. Distributing headroom across all pools fixes the chicken-and-egg.
        allocator.setRebalanceInterval(60);
        allocator.setRootLimit(100 * 1024 * 1024);
        allocator.getOrCreatePool("zero-min", 0L, 100 * 1024 * 1024);
        try {
            allocator.rebalance();
            BufferAllocator pool = allocator.getPoolAllocator("zero-min");
            assertTrue("Zero-min pool should receive headroom, got " + pool.getLimit(), pool.getLimit() > 0);
        } finally {
            allocator.setRebalanceInterval(0);
        }
    }

    public void testRebalanceNeverDropsBelowCurrentAllocation() {
        allocator.setRootLimit(50 * 1024 * 1024);
        allocator.getOrCreatePool("busy", 10 * 1024 * 1024);

        BufferAllocator pool = allocator.getPoolAllocator("busy");
        BufferAllocator child = pool.newChildAllocator("w", 0, 10 * 1024 * 1024);
        var buf = child.buffer(8 * 1024 * 1024); // 8 MB allocated

        try {
            allocator.rebalance();
            assertTrue("Limit should never drop below current allocation", pool.getLimit() >= pool.getAllocatedMemory());
        } finally {
            buf.close();
            child.close();
        }
    }

    public void testRebalanceWithNoPools() {
        // Should not throw
        allocator.rebalance();
    }

    public void testInitialLimitIsMaxWhenRebalancerDisabled() {
        // Default tearDown allocator has rebalancer disabled (interval=0).
        NativeAllocator.PoolHandle handle = allocator.getOrCreatePool("burst", 10 * 1024 * 1024, 100 * 1024 * 1024);
        // With the rebalancer off, pools must start at their max so consumers can allocate
        // immediately. Otherwise default-configured pools (min=0) would reject everything.
        assertEquals(100 * 1024 * 1024, handle.limit());
    }

    public void testInitialLimitIsMinWhenRebalancerEnabled() {
        // Enabling the rebalancer reverts to the original "guarantee + burst" semantics:
        // pools start at min and grow via the next rebalance tick.
        allocator.setRebalanceInterval(60); // any positive value enables the flag
        NativeAllocator.PoolHandle handle = allocator.getOrCreatePool("guaranteed", 10 * 1024 * 1024, 100 * 1024 * 1024);
        assertEquals(10 * 1024 * 1024, handle.limit());
        // Disable so subsequent tests aren't affected by the scheduled task.
        allocator.setRebalanceInterval(0);
    }

    public void testCloseReleasesAllPools() {
        allocator.getOrCreatePool("close-test", 10 * 1024 * 1024);
        allocator.close();
        assertTrue(allocator.getPoolNames().isEmpty());

        // Recreate for tearDown
        allocator = new ArrowNativeAllocator(1024L * 1024 * 1024);
    }

    public void testSetPoolMinRaisesLiveLimitWhenRebalancerOff() {
        // setPoolMin must affect the live BufferAllocator immediately, not just the
        // poolMins map. Otherwise it's a Dynamic setting that returns HTTP 200 and
        // does nothing observable until the operator also enables the rebalancer.
        allocator.setRootLimit(100 * 1024 * 1024);
        allocator.getOrCreatePool("p", 0L, 100 * 1024 * 1024);
        BufferAllocator pool = allocator.getPoolAllocator("p");

        long startLimit = pool.getLimit();
        allocator.setPoolMin("p", 50 * 1024 * 1024);

        long afterMinUpdate = pool.getLimit();
        assertTrue(
            "setPoolMin should raise live limit to at least the new min (was " + startLimit + ", now " + afterMinUpdate + ")",
            afterMinUpdate >= 50 * 1024 * 1024
        );
    }

    public void testSetPoolMinDoesNotShrinkLiveLimit() {
        // Dropping the min must not shrink an in-flight pool — the rebalancer is the
        // only path that reduces limits, so a min change on its own should never
        // reclaim capacity.
        allocator.setRootLimit(100 * 1024 * 1024);
        allocator.getOrCreatePool("p", 0L, 100 * 1024 * 1024);
        BufferAllocator pool = allocator.getPoolAllocator("p");
        long startLimit = pool.getLimit();

        allocator.setPoolMin("p", 1L);
        assertEquals("dropping min must not shrink live limit", startLimit, pool.getLimit());
    }
}
