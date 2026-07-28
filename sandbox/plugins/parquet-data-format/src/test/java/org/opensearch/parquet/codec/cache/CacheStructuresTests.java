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
import java.lang.foreign.ValueLayout;

/**
 * Unit tests for the layered-cache structures (task 3.4): {@link ColumnPageIndex} binary
 * search + all-nulls detection (Layer 3/4), {@link PageCache} presence bit-test (Layer 2),
 * and {@link BufferPool} slot reuse/growth (Layer 5). These are pure-Java and do not require
 * the native library.
 */
public class CacheStructuresTests extends OpenSearchTestCase {

    // ── ColumnPageIndex: pageForRow binary search across page boundaries ──

    public void testPageForRowAcrossBoundaries() {
        // 3 pages: rows [0,4), [4,10), [10,15). totalRows = 15.
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0, 4, 10 },
            new long[] { 0, 0, 0 },
            new int[] { 0, 0, 0 },
            new long[] { -1, -1, -1 },
            new long[] { 0, 0, 0 },
            new long[] { 0, 0, 0 },
            15
        );
        assertEquals(3, idx.pageCount());

        // First page [0,4)
        assertEquals(0, idx.pageForRow(0));
        assertEquals(0, idx.pageForRow(3));
        // Boundary into second page
        assertEquals(1, idx.pageForRow(4));
        assertEquals(1, idx.pageForRow(9));
        // Boundary into third page
        assertEquals(2, idx.pageForRow(10));
        assertEquals(2, idx.pageForRow(14));

        // Out of range
        assertEquals(-1, idx.pageForRow(-1));
        assertEquals(-1, idx.pageForRow(15));
        assertEquals(-1, idx.pageForRow(100));
    }

    public void testNumRowsOfIncludingLastPage() {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0, 4, 10 },
            new long[] { 0, 0, 0 },
            new int[] { 0, 0, 0 },
            new long[] { -1, -1, -1 },
            new long[] { 0, 0, 0 },
            new long[] { 0, 0, 0 },
            15
        );
        assertEquals(4, idx.numRowsOf(0));
        assertEquals(6, idx.numRowsOf(1));
        assertEquals(5, idx.numRowsOf(2)); // last page derives length from totalRows
    }

    public void testSinglePageIndex() {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0 },
            new long[] { 0 },
            new int[] { 0 },
            new long[] { -1 },
            new long[] { 0 },
            new long[] { 0 },
            8
        );
        assertEquals(0, idx.pageForRow(0));
        assertEquals(0, idx.pageForRow(7));
        assertEquals(-1, idx.pageForRow(8));
        assertEquals(8, idx.numRowsOf(0));
    }

    // ── ColumnPageIndex: Layer 4 all-nulls detection ──

    public void testIsAllNulls() {
        // page 0: [0,4) all null (nullCount == 4); page 1: [4,10) no nulls; page 2: [10,15) unknown.
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0, 4, 10 },
            new long[] { 0, 0, 0 },
            new int[] { 0, 0, 0 },
            new long[] { 4, 0, -1 },
            new long[] { 0, 0, 0 },
            new long[] { 0, 0, 0 },
            15
        );
        assertTrue("page 0 is entirely null", idx.isAllNulls(0));
        assertFalse("page 1 has no nulls", idx.isAllNulls(1));
        assertFalse("page 2 null count unknown -> not skippable", idx.isAllNulls(2));
    }

    public void testParallelArrayLengthValidation() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ColumnPageIndex(
                new long[] { 0, 4 },
                new long[] { 0 },
                new int[] { 0 },
                new long[] { 0 },
                new long[] { 0 },
                new long[] { 0 },
                4
            )
        );
    }

    // ── PageCache: Layer 2 presence bit-test + value lookup ──

    public void testPresenceBitTestAndValueLookup() {
        PageCache pc = new PageCache();
        pc.firstRow = 4;
        pc.lastRow = 9; // 6 rows
        pc.values = MemorySegment.ofArray(new long[] { 40, 0, 60, 70, 0, 90 });
        // present rows (relative): 0,2,3,5 -> bits 0b101101 = 45
        pc.presenceBits = MemorySegment.ofArray(new long[] { 0b101101L });

        assertEquals(6, pc.rowCount());
        assertTrue(pc.contains(4));
        assertTrue(pc.contains(9));
        assertFalse(pc.contains(3));
        assertFalse(pc.contains(10));

        assertTrue(pc.isPresent(4)); // rel 0
        assertFalse(pc.isPresent(5)); // rel 1
        assertTrue(pc.isPresent(6)); // rel 2
        assertTrue(pc.isPresent(7)); // rel 3
        assertFalse(pc.isPresent(8)); // rel 4
        assertTrue(pc.isPresent(9)); // rel 5

        assertEquals(40L, pc.valueAt(4));
        assertEquals(60L, pc.valueAt(6));
        assertEquals(90L, pc.valueAt(9));
    }

    public void testPresenceBitTestAcrossWordBoundary() {
        PageCache pc = new PageCache();
        pc.firstRow = 0;
        pc.lastRow = 127; // 128 rows -> 2 presence words
        long[] presenceBits = new long[2];
        // Mark rows 63 and 64 present (straddling the 64-bit word boundary).
        presenceBits[0] = 1L << 63;
        presenceBits[1] = 1L;
        pc.presenceBits = MemorySegment.ofArray(presenceBits);

        assertFalse(pc.isPresent(62));
        assertTrue(pc.isPresent(63));
        assertTrue(pc.isPresent(64));
        assertFalse(pc.isPresent(65));
    }

    // ── BufferPool: Layer 5 slot reuse + monotonic growth ──

    public void testBufferPoolReusesSlotWhenSizeFits() {
        try (BufferPool pool = new BufferPool()) {
            MemorySegment a = pool.longs("slot", 8);
            MemorySegment b = pool.longs("slot", 4); // smaller -> same backing segment
            assertSame("a smaller request must reuse the same backing segment", a, b);
        }
    }

    public void testBufferPoolGrowsSlotMonotonically() {
        try (BufferPool pool = new BufferPool()) {
            MemorySegment small = pool.longs("slot", 2);
            long smallSize = small.byteSize();
            MemorySegment large = pool.longs("slot", 1000); // forces growth
            assertTrue("grown segment must be larger", large.byteSize() > smallSize);
            assertTrue("grown segment must hold the requested size", large.byteSize() >= 1000 * ValueLayout.JAVA_LONG.byteSize());

            // A subsequent smaller request reuses the grown segment (no shrink).
            MemorySegment reused = pool.longs("slot", 2);
            assertSame(large, reused);
        }
    }

    public void testBufferPoolDistinctSlotsAreIndependent() {
        try (BufferPool pool = new BufferPool()) {
            MemorySegment a = pool.longOut("present");
            MemorySegment b = pool.longOut("len");
            assertNotSame("distinct slot names must yield distinct segments", a, b);
        }
    }

    public void testBufferPoolRejectsUseAfterClose() {
        BufferPool pool = new BufferPool();
        pool.close();
        expectThrows(IllegalStateException.class, () -> pool.longs("slot", 1));
    }
}
