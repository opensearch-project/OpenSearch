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
import org.opensearch.arrow.spi.NativeAllocatorPoolStats;
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
        assertEquals(0, poolStats.getChildCount());
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

    public void testRebalanceGivesHeadroomToActivePool() {
        allocator.setRootLimit(100 * 1024 * 1024);
        allocator.getOrCreatePool("active", 10 * 1024 * 1024, 100 * 1024 * 1024);
        allocator.getOrCreatePool("idle", 10 * 1024 * 1024, 100 * 1024 * 1024);

        // Simulate activity: allocate in "active" pool (allocated > 0 = active)
        BufferAllocator activeAlloc = allocator.getPoolAllocator("active");
        BufferAllocator child = activeAlloc.newChildAllocator("worker", 0, 100 * 1024 * 1024);
        var buf = child.buffer(5 * 1024 * 1024);

        try {
            allocator.rebalance();

            // Active pool should get bonus headroom beyond its min
            long activeLimit = activeAlloc.getLimit();
            assertTrue("Active pool limit should exceed min after rebalance, got " + activeLimit, activeLimit > 10 * 1024 * 1024);

            // Idle pool should be at its min
            long idleLimit = allocator.getPoolAllocator("idle").getLimit();
            assertEquals(10 * 1024 * 1024, idleLimit);
        } finally {
            buf.close();
            child.close();
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

    public void testCloseReleasesAllPools() {
        allocator.getOrCreatePool("close-test", 10 * 1024 * 1024);
        allocator.close();
        assertTrue(allocator.getPoolNames().isEmpty());

        // Recreate for tearDown
        allocator = new ArrowNativeAllocator(1024L * 1024 * 1024);
    }
}
