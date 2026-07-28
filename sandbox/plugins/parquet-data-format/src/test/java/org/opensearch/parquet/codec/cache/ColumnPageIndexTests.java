/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link ColumnPageIndex} (task 3.4): page-for-row binary search across page
 * boundaries, row-count derivation, and the Layer-4 all-nulls detection.
 */
public class ColumnPageIndexTests extends OpenSearchTestCase {

    /** Three pages: rows [0,4), [4,8), [8,10) — total 10 rows. */
    private ColumnPageIndex threePage() {
        return new ColumnPageIndex(
            new long[] { 0, 4, 8 },          // firstRowOf
            new long[] { 0, 100, 200 },      // fileOffsetOf
            new int[] { 50, 50, 25 },        // compressedSizeOf
            new long[] { 0, 0, 0 },          // nullCountOf
            new long[] { 0, 0, 0 },          // minOf
            new long[] { 0, 0, 0 },          // maxOf
            10                               // totalRows
        );
    }

    public void testPageCountAndRowsPerPage() {
        ColumnPageIndex idx = threePage();
        assertEquals(3, idx.pageCount());
        assertEquals(4L, idx.numRowsOf(0));
        assertEquals(4L, idx.numRowsOf(1));
        assertEquals(2L, idx.numRowsOf(2)); // last page: totalRows - firstRow = 10 - 8
        assertEquals(10L, idx.totalRows());
    }

    public void testPageForRowAcrossBoundaries() {
        ColumnPageIndex idx = threePage();
        // page 0: rows 0..3
        assertEquals(0, idx.pageForRow(0));
        assertEquals(0, idx.pageForRow(3));
        // page 1: rows 4..7 (boundary at 4)
        assertEquals(1, idx.pageForRow(4));
        assertEquals(1, idx.pageForRow(7));
        // page 2: rows 8..9 (boundary at 8)
        assertEquals(2, idx.pageForRow(8));
        assertEquals(2, idx.pageForRow(9));
    }

    public void testPageForRowOutOfRange() {
        ColumnPageIndex idx = threePage();
        assertEquals(-1, idx.pageForRow(-1));
        assertEquals(-1, idx.pageForRow(10));
        assertEquals(-1, idx.pageForRow(99));
    }

    public void testSinglePage() {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0 },
            new long[] { 0 },
            new int[] { 10 },
            new long[] { -1 },
            new long[] { 0 },
            new long[] { 0 },
            5
        );
        assertEquals(1, idx.pageCount());
        assertEquals(5L, idx.numRowsOf(0));
        for (long r = 0; r < 5; r++) {
            assertEquals("row " + r, 0, idx.pageForRow(r));
        }
        assertEquals(-1, idx.pageForRow(5));
    }

    public void testIsAllNulls() {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0, 4, 8 },
            new long[] { 0, 0, 0 },
            new int[] { 0, 0, 0 },
            new long[] { 4, 0, -1 },         // page0 all-null (4 of 4), page1 none, page2 unknown
            new long[] { 0, 0, 0 },
            new long[] { 0, 0, 0 },
            10
        );
        assertTrue("page 0 (nullCount==rows) should be all-nulls", idx.isAllNulls(0));
        assertFalse("page 1 (nullCount 0) should not be all-nulls", idx.isAllNulls(1));
        assertFalse("page 2 (nullCount unknown) should not be all-nulls", idx.isAllNulls(2));
    }

    public void testMismatchedArrayLengthsRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ColumnPageIndex(
                new long[] { 0, 4 },
                new long[] { 0 }, // wrong length
                new int[] { 0, 0 },
                new long[] { 0, 0 },
                new long[] { 0, 0 },
                new long[] { 0, 0 },
                8
            )
        );
    }

    public void testManyPagesBinarySearch() {
        int pages = 1000;
        long[] firstRowOf = new long[pages];
        long[] zerosL = new long[pages];
        int[] zerosI = new int[pages];
        for (int p = 0; p < pages; p++) {
            firstRowOf[p] = (long) p * 8;
        }
        ColumnPageIndex idx = new ColumnPageIndex(
            firstRowOf,
            zerosL,
            zerosI,
            zerosL.clone(),
            zerosL.clone(),
            zerosL.clone(),
            (long) pages * 8
        );
        // Probe a random row in every page and confirm the binary search lands on it.
        for (int p = 0; p < pages; p++) {
            long row = (long) p * 8 + randomIntBetween(0, 7);
            assertEquals("row " + row, p, idx.pageForRow(row));
        }
    }
}
