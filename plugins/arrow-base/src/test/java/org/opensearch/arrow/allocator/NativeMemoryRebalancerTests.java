/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.spi.PoolGroup;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class NativeMemoryRebalancerTests extends OpenSearchTestCase {

    private static final long MB = 1024L * 1024;
    private static final long BUDGET = 100 * MB;

    private ArrowNativeAllocator allocator;
    private NativeMemoryRebalancer rebalancer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new ArrowNativeAllocator();
        allocator.setBudget(BUDGET);
        rebalancer = new NativeMemoryRebalancer(allocator, () -> BUDGET, 0.75, 0.50, 0.10);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testPoolsStartAtMax() {
        allocator.getOrCreatePool("a", 5 * MB, 40 * MB, null);
        allocator.getOrCreatePool("b", 10 * MB, 50 * MB, null);

        assertEquals(40 * MB, allocator.getPoolAllocator("a").getLimit());
        assertEquals(50 * MB, allocator.getPoolAllocator("b").getLimit());
    }

    public void testShrinksIdlePool() {
        allocator.getOrCreatePool("idle", 5 * MB, 50 * MB, null);
        allocator.getOrCreatePool("pressured", 5 * MB, 50 * MB, null);

        BufferAllocator pressuredPool = allocator.getPoolAllocator("pressured");
        ArrowBuf buf = pressuredPool.buffer((long) (50 * MB * 0.8));

        try {
            long idleLimitBefore = allocator.getPoolAllocator("idle").getLimit();
            rebalancer.rebalance();
            long idleLimitAfter = allocator.getPoolAllocator("idle").getLimit();
            assertTrue("Idle pool should shrink, was " + idleLimitBefore + " now " + idleLimitAfter, idleLimitAfter < idleLimitBefore);
        } finally {
            buf.close();
        }
    }

    public void testGrowsPressuredPoolAboveMax() {
        allocator.getOrCreatePool("idle", 5 * MB, 50 * MB, null);
        allocator.getOrCreatePool("pressured", 5 * MB, 20 * MB, null);

        BufferAllocator pressuredPool = allocator.getPoolAllocator("pressured");
        ArrowBuf buf = pressuredPool.buffer((long) (20 * MB * 0.8));

        try {
            rebalancer.rebalance();
            long pressuredLimit = pressuredPool.getLimit();
            assertTrue("Pressured pool should grow above max (20MB), got " + pressuredLimit, pressuredLimit > 20 * MB);
        } finally {
            buf.close();
        }
    }

    public void testNeverDropsBelowMin() {
        allocator.getOrCreatePool("floored", 10 * MB, 50 * MB, null);
        allocator.getOrCreatePool("pressured", 5 * MB, 50 * MB, null);

        BufferAllocator pressuredPool = allocator.getPoolAllocator("pressured");
        ArrowBuf buf = pressuredPool.buffer((long) (50 * MB * 0.8));

        try {
            for (int i = 0; i < 20; i++) {
                rebalancer.rebalance();
            }
            long flooredLimit = allocator.getPoolAllocator("floored").getLimit();
            assertTrue("Pool limit (" + flooredLimit + ") should not drop below min (10MB)", flooredLimit >= 10 * MB);
        } finally {
            buf.close();
        }
    }

    public void testNoActionWhenNoPressure() {
        allocator.getOrCreatePool("a", 5 * MB, 50 * MB, null);
        allocator.getOrCreatePool("b", 5 * MB, 50 * MB, null);

        long limitA = allocator.getPoolAllocator("a").getLimit();
        long limitB = allocator.getPoolAllocator("b").getLimit();

        rebalancer.rebalance();

        assertEquals(limitA, allocator.getPoolAllocator("a").getLimit());
        assertEquals(limitB, allocator.getPoolAllocator("b").getLimit());
    }

    public void testResetAllPoolsToMax() {
        allocator.getOrCreatePool("a", 5 * MB, 40 * MB, null);
        allocator.getOrCreatePool("b", 5 * MB, 50 * MB, null);

        BufferAllocator bPool = allocator.getPoolAllocator("b");
        ArrowBuf buf = bPool.buffer((long) (50 * MB * 0.8));
        rebalancer.rebalance();
        buf.close();

        allocator.resetAllPoolsToMax();

        assertEquals(40 * MB, allocator.getPoolAllocator("a").getLimit());
        assertEquals(50 * MB, allocator.getPoolAllocator("b").getLimit());
    }

    public void testSumLimitsNeverExceedsBudget() {
        allocator.getOrCreatePool("p1", 5 * MB, 30 * MB, null);
        allocator.getOrCreatePool("p2", 5 * MB, 30 * MB, null);
        allocator.getOrCreatePool("p3", 5 * MB, 30 * MB, null);

        List<ArrowBuf> bufs = new ArrayList<>();
        for (String name : new String[] { "p1", "p2", "p3" }) {
            BufferAllocator pool = allocator.getPoolAllocator(name);
            bufs.add(pool.buffer((long) (pool.getLimit() * 0.8)));
        }

        try {
            for (int i = 0; i < 10; i++) {
                rebalancer.rebalance();
            }

            long sumLimits = 0;
            for (String name : new String[] { "p1", "p2", "p3" }) {
                sumLimits += allocator.getPoolAllocator(name).getLimit();
            }
            assertTrue("Sum of limits (" + sumLimits + ") should not exceed budget (" + BUDGET + ")", sumLimits <= BUDGET);
        } finally {
            bufs.forEach(ArrowBuf::close);
        }
    }

    public void testUnmanagedPoolUntouchedByRebalance() {
        // A pressured managed pool that would normally receive freed capacity...
        allocator.getOrCreatePool("pressured", 5 * MB, 20 * MB, PoolGroup.INDEXING);
        // ...and the special unbounded query pool, which is idle (0 allocated).
        allocator.registerUnmanagedPool("query", PoolGroup.SEARCH);

        BufferAllocator pressuredPool = allocator.getPoolAllocator("pressured");
        ArrowBuf buf = pressuredPool.buffer((long) (20 * MB * 0.9)); // >75% → pressured
        try {
            long queryLimitBefore = allocator.getPoolAllocator("query").getLimit();
            rebalancer.rebalance();
            long queryLimitAfter = allocator.getPoolAllocator("query").getLimit();

            // The unmanaged pool's limit is unchanged (still Long.MAX_VALUE) — it was neither
            // shrunk as an "idle" pool nor otherwise touched.
            assertEquals("unmanaged pool must not be resized by the rebalancer", queryLimitBefore, queryLimitAfter);
            assertEquals(Long.MAX_VALUE, queryLimitAfter);
        } finally {
            buf.close();
        }
    }
}
