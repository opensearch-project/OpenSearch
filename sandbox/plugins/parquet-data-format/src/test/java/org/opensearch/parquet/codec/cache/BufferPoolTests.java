/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.MemorySegment;

/**
 * Unit tests for {@link BufferPool} (task 3.4): per-role reuse, monotonic grow, distinct
 * roles, and post-close rejection.
 */
public class BufferPoolTests extends OpenSearchTestCase {

    public void testSameRoleReusedWhenLargeEnough() {
        try (BufferPool pool = new BufferPool()) {
            MemorySegment a = pool.bytes("scratch", 128);
            MemorySegment b = pool.bytes("scratch", 64); // smaller request — must reuse
            assertSame("smaller request should reuse the same backing segment", a, b);
            assertEquals(1, pool.slotCount());
        }
    }

    public void testRoleGrowsMonotonically() {
        try (BufferPool pool = new BufferPool()) {
            MemorySegment small = pool.bytes("scratch", 64);
            assertTrue(small.byteSize() >= 64);
            MemorySegment grown = pool.bytes("scratch", 4096); // larger — must grow
            assertTrue("grown segment must be at least the requested size", grown.byteSize() >= 4096);
            assertNotSame("growing should replace the backing segment", small, grown);
            // After growing, a smaller request reuses the grown (larger) segment.
            MemorySegment reused = pool.bytes("scratch", 100);
            assertSame(grown, reused);
        }
    }

    public void testDistinctRolesAreIndependent() {
        try (BufferPool pool = new BufferPool()) {
            MemorySegment values = pool.longs("values", 10);
            MemorySegment presence = pool.longs("presence", 4);
            MemorySegment offsets = pool.ints("offsets", 11);
            assertNotSame(values, presence);
            assertNotSame(values, offsets);
            assertNotSame(presence, offsets);
            assertEquals(3, pool.slotCount());
            // Re-fetching a role returns its own segment, unaffected by other roles.
            assertSame(values, pool.longs("values", 10));
        }
    }

    public void testLongsAndIntsSizing() {
        try (BufferPool pool = new BufferPool()) {
            assertTrue(pool.longs("a", 8).byteSize() >= 8 * Long.BYTES);
            assertTrue(pool.ints("b", 16).byteSize() >= 16 * Integer.BYTES);
            assertTrue("zero count still yields a usable segment", pool.longs("c", 0).byteSize() >= Long.BYTES);
        }
    }

    public void testUseAfterCloseThrows() {
        BufferPool pool = new BufferPool();
        pool.bytes("scratch", 16);
        pool.close();
        expectThrows(IllegalStateException.class, () -> pool.bytes("scratch", 16));
        // close is idempotent
        pool.close();
    }
}
