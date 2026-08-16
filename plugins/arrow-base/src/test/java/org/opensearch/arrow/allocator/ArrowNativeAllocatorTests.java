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
import org.opensearch.arrow.spi.PoolGroup;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowNativeAllocatorTests extends OpenSearchTestCase {

    private ArrowNativeAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new ArrowNativeAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testCreatePool() {
        NativeAllocator.PoolHandle handle = allocator.getOrCreatePool("test-pool", 0L, 100 * 1024 * 1024, null);
        assertNotNull(handle);
        assertEquals(100 * 1024 * 1024, handle.limit());
        assertEquals(0, handle.allocatedBytes());
    }

    public void testGetOrCreatePoolIdempotent() {
        NativeAllocator.PoolHandle first = allocator.getOrCreatePool("idempotent", 0L, 50 * 1024 * 1024, null);
        NativeAllocator.PoolHandle second = allocator.getOrCreatePool("idempotent", 0L, 999 * 1024 * 1024, null);
        assertSame(first, second);
        assertEquals(50 * 1024 * 1024, second.limit());
    }

    public void testPoolChildAllocation() {
        allocator.getOrCreatePool("parent", 0L, 200 * 1024 * 1024, null);
        BufferAllocator child = allocator.getPoolAllocator("parent").newChildAllocator("child-1", 0, 50 * 1024 * 1024);
        try {
            child.buffer(1024).close();
            assertTrue(allocator.getPoolAllocator("parent").getAllocatedMemory() == 0);
        } finally {
            child.close();
        }
    }

    public void testSetPoolLimit() {
        allocator.getOrCreatePool("resizable", 0L, 100 * 1024 * 1024, null);
        allocator.setPoolLimit("resizable", 200 * 1024 * 1024);
        assertEquals(200 * 1024 * 1024, allocator.getPoolAllocator("resizable").getLimit());
    }

    public void testSetPoolLimitNonExistent() {
        expectThrows(IllegalStateException.class, () -> allocator.setPoolLimit("ghost", 100));
    }

    public void testGetPoolAllocatorNonExistent() {
        expectThrows(IllegalStateException.class, () -> allocator.getPoolAllocator("ghost"));
    }

    public void testStats() {
        allocator.getOrCreatePool("stats-pool", 0L, 64 * 1024 * 1024, PoolGroup.SEARCH);
        NativeAllocatorPoolStats stats = allocator.stats();

        assertNotNull(stats);
        assertEquals(-1, stats.getNativeAllocatedBytes());
        assertEquals(-1, stats.getNativeResidentBytes());
        assertEquals(1, stats.getPools().size());

        NativeAllocatorPoolStats.PoolStats poolStats = stats.getPools().get(0);
        assertEquals("stats-pool", poolStats.getName());
        assertEquals(64 * 1024 * 1024, poolStats.getLimitBytes());
        assertEquals(0, poolStats.getAllocatedBytes());
    }

    public void testStatsMultiplePools() {
        allocator.getOrCreatePool("pool-a", 0L, 100 * 1024 * 1024, null);
        allocator.getOrCreatePool("pool-b", 0L, 200 * 1024 * 1024, null);

        NativeAllocatorPoolStats stats = allocator.stats();
        assertEquals(2, stats.getPools().size());
    }

    public void testGetPoolNames() {
        allocator.getOrCreatePool("alpha", 0L, 10 * 1024 * 1024, null);
        allocator.getOrCreatePool("beta", 0L, 20 * 1024 * 1024, null);

        assertTrue(allocator.getPoolNames().contains("alpha"));
        assertTrue(allocator.getPoolNames().contains("beta"));
        assertEquals(2, allocator.getPoolNames().size());
    }

    public void testInitialLimitIsMax() {
        NativeAllocator.PoolHandle handle = allocator.getOrCreatePool("burst", 10 * 1024 * 1024, 100 * 1024 * 1024, null);
        assertEquals(100 * 1024 * 1024, handle.limit());
    }

    public void testCloseReleasesAllPools() {
        allocator.getOrCreatePool("close-test", 0L, 10 * 1024 * 1024, null);
        allocator.close();
        assertTrue(allocator.getPoolNames().isEmpty());

        // Recreate for tearDown
        allocator = new ArrowNativeAllocator();
    }

    public void testSetPoolMinRaisesLiveLimitWhenNeeded() {
        allocator.getOrCreatePool("p", 0L, 100 * 1024 * 1024, null);
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
        allocator.getOrCreatePool("p", 0L, 100 * 1024 * 1024, null);
        BufferAllocator pool = allocator.getPoolAllocator("p");
        long startLimit = pool.getLimit();

        allocator.setPoolMin("p", 1L);
        assertEquals("dropping min must not shrink live limit", startLimit, pool.getLimit());
    }

    public void testPoolGroupLimitListenerFiresWithCorrectSum() {
        allocator.getOrCreatePool("ingest", 0L, 80 * 1024 * 1024, PoolGroup.INDEXING);
        allocator.getOrCreatePool("write", 0L, 50 * 1024 * 1024, PoolGroup.INDEXING);

        java.util.concurrent.atomic.AtomicLong received = new java.util.concurrent.atomic.AtomicLong(-1);
        allocator.addPoolGroupLimitListener(PoolGroup.INDEXING, received::set);

        // Change limits
        allocator.setPoolEffectiveLimit("ingest", 60 * 1024 * 1024);
        allocator.setPoolEffectiveLimit("write", 40 * 1024 * 1024);

        // Fire after all changes
        allocator.firePoolGroupListeners(PoolGroup.INDEXING);

        // Listener should receive sum of effective limits: 60MB + 40MB = 100MB
        assertEquals(100L * 1024 * 1024, received.get());
    }

    public void testPoolGroupLimitListenerNotFiredForOtherGroups() {
        allocator.getOrCreatePool("flight", 0L, 50 * 1024 * 1024, PoolGroup.TRANSPORT);
        allocator.getOrCreatePool("ingest", 0L, 80 * 1024 * 1024, PoolGroup.INDEXING);

        java.util.concurrent.atomic.AtomicLong transportReceived = new java.util.concurrent.atomic.AtomicLong(-1);
        allocator.addPoolGroupLimitListener(PoolGroup.TRANSPORT, transportReceived::set);

        // Fire INDEXING group — should NOT trigger TRANSPORT listener
        allocator.firePoolGroupListeners(PoolGroup.INDEXING);
        assertEquals(-1, transportReceived.get());

        // Fire TRANSPORT group — should trigger
        allocator.firePoolGroupListeners(PoolGroup.TRANSPORT);
        assertEquals(50L * 1024 * 1024, transportReceived.get());
    }
}
