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

import java.util.concurrent.atomic.AtomicLong;

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

        AtomicLong received = new AtomicLong(-1);
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

        AtomicLong transportReceived = new AtomicLong(-1);
        allocator.addPoolGroupLimitListener(PoolGroup.TRANSPORT, transportReceived::set);

        // Fire INDEXING group — should NOT trigger TRANSPORT listener
        allocator.firePoolGroupListeners(PoolGroup.INDEXING);
        assertEquals(-1, transportReceived.get());

        // Fire TRANSPORT group — should trigger
        allocator.firePoolGroupListeners(PoolGroup.TRANSPORT);
        assertEquals(50L * 1024 * 1024, transportReceived.get());
    }

    // ─── Unmanaged (special, unbounded) pool ───────────────────────────────────────

    public void testRegisterUnmanagedPoolIsUnboundedAndFlagged() {
        NativeAllocator.PoolHandle handle = allocator.registerUnmanagedPool("query", PoolGroup.SEARCH);
        assertNotNull(handle);
        assertEquals("unmanaged pool is fixed at Long.MAX_VALUE", Long.MAX_VALUE, handle.limit());
        assertTrue(allocator.isUnmanagedPool("query"));
        assertFalse(allocator.isUnmanagedPool("some-other-pool"));
    }

    public void testRegisterUnmanagedPoolRejectsExistingManagedPool() {
        // A pool created as managed (bounded child allocator) must not be flippable to unmanaged.
        allocator.getOrCreatePool("query", 0L, 100 * 1024 * 1024, PoolGroup.SEARCH);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> allocator.registerUnmanagedPool("query", PoolGroup.SEARCH)
        );
        assertTrue(e.getMessage().contains("already exists as a managed pool"));
        assertFalse("must not be flagged unmanaged after a rejected re-register", allocator.isUnmanagedPool("query"));
    }

    public void testUnmanagedPoolExcludedFromManagedPoolNames() {
        allocator.getOrCreatePool("flight", 0L, 50 * 1024 * 1024, PoolGroup.TRANSPORT);
        allocator.registerUnmanagedPool("query", PoolGroup.SEARCH);

        // Both appear in the full set, but only the managed one is in getManagedPoolNames.
        assertTrue(allocator.getAllPoolNames().contains("query"));
        assertTrue(allocator.getAllPoolNames().contains("flight"));
        assertTrue(allocator.getManagedPoolNames().contains("flight"));
        assertFalse(
            "unmanaged query pool must be excluded from the rebalancer's managed set",
            allocator.getManagedPoolNames().contains("query")
        );
    }

    public void testUnmanagedPoolExcludedFromBudgetValidation() {
        // Budget only fits the flight pool's max; the unbounded query pool must NOT be summed
        // against the budget (else its Long.MAX_VALUE would trip validation).
        allocator.setBudget(100 * 1024 * 1024);
        allocator.getOrCreatePool("flight", 0L, 50 * 1024 * 1024, PoolGroup.TRANSPORT);
        // Registering the unbounded pool must not throw despite Long.MAX_VALUE > budget.
        allocator.registerUnmanagedPool("query", PoolGroup.SEARCH);

        // A managed pool whose max would exceed the remaining budget still trips — proving the
        // unmanaged pool simply isn't counted, not that validation was disabled entirely.
        expectThrows(IllegalArgumentException.class, () -> allocator.getOrCreatePool("ingest", 0L, 80 * 1024 * 1024, PoolGroup.INDEXING));
    }

    public void testUnmanagedPoolExcludedFromPoolGroupSum() {
        // A managed SEARCH pool + an unmanaged SEARCH pool: the group sum must reflect only the
        // managed one, not the unmanaged pool's Long.MAX_VALUE limit.
        allocator.getOrCreatePool("datafusion", 0L, 70 * 1024 * 1024, PoolGroup.SEARCH);
        allocator.registerUnmanagedPool("query", PoolGroup.SEARCH);

        AtomicLong received = new AtomicLong(-1);
        allocator.addPoolGroupLimitListener(PoolGroup.SEARCH, received::set);
        allocator.firePoolGroupListeners(PoolGroup.SEARCH);

        assertEquals("group sum must exclude the unmanaged pool", 70L * 1024 * 1024, received.get());
    }

    public void testUnmanagedPoolLimitUntouchedByResetAllPoolsToMax() {
        allocator.registerUnmanagedPool("query", PoolGroup.SEARCH);
        allocator.resetAllPoolsToMax();
        assertEquals(Long.MAX_VALUE, allocator.getPoolAllocator("query").getLimit());
    }
}
