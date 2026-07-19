/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.parquet.codec.cache.ColumnPageIndex;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link ParquetDocValuesSkipper}: the DocValuesSkipper contract (advance
 * semantics, sentinel doc IDs) and range-driven page skipping over a synthetic page index.
 */
public class ParquetDocValuesSkipperTests extends OpenSearchTestCase {

    /**
     * Three 100-row pages with disjoint value ranges:
     * page 0 rows [0,99] values [10,20], page 1 rows [100,199] values [50,60],
     * page 2 rows [200,299] values [90,100]. No nulls.
     */
    private static ColumnPageIndex threePages() {
        return new ColumnPageIndex(
            new long[] { 0, 100, 200 },
            new long[] { 0, 0, 0 },
            new int[] { 0, 0, 0 },
            new long[] { 0, 0, 0 },
            new long[] { 10, 50, 90 },
            new long[] { 20, 60, 100 },
            300
        );
    }

    public void testInitialStateAndAdvance() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        assertEquals(-1, skipper.minDocID(0));
        assertEquals(-1, skipper.maxDocID(0));
        assertEquals(1, skipper.numLevels());

        skipper.advance(0);
        assertEquals(0, skipper.minDocID(0));
        assertEquals(99, skipper.maxDocID(0));
        assertEquals(10L, skipper.minValue(0));
        assertEquals(20L, skipper.maxValue(0));
        assertEquals(100, skipper.docCount(0));

        skipper.advance(150);
        assertEquals(100, skipper.minDocID(0));
        assertEquals(199, skipper.maxDocID(0));
        assertEquals(50L, skipper.minValue(0));
        assertEquals(60L, skipper.maxValue(0));
    }

    public void testExhaustion() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        skipper.advance(300);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.minDocID(0));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.maxDocID(0));
    }

    public void testGlobalStats() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        assertEquals(10L, skipper.minValue());
        assertEquals(100L, skipper.maxValue());
        assertEquals(300, skipper.docCount());
    }

    /** The base-class range advance must land on the first page intersecting the value range. */
    public void testRangeAdvanceSkipsNonIntersectingPages() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        // [55, 58] intersects only page 1.
        skipper.advance(55L, 58L);
        assertEquals(100, skipper.minDocID(0));
        assertEquals(199, skipper.maxDocID(0));

        // A range beyond every page exhausts the skipper.
        DocValuesSkipper skipper2 = new ParquetDocValuesSkipper(threePages(), 300);
        skipper2.advance(200L, 300L);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper2.minDocID(0));
    }

    /** Pages with the unknown-stats sentinel must intersect every range (never wrongly skipped). */
    public void testUnknownStatsPageIsNeverSkipped() throws Exception {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0, 100 },
            new long[] { 0, 0 },
            new int[] { 0, 0 },
            new long[] { 0, -1 },
            new long[] { 10, Long.MIN_VALUE },
            new long[] { 20, Long.MAX_VALUE },
            200
        );
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(idx, 200);
        // Range [500, 600] excludes page 0 (max 20) but must land on the unknown-stats page.
        skipper.advance(500L, 600L);
        assertEquals(100, skipper.minDocID(0));
        assertEquals(199, skipper.maxDocID(0));
        // Unknown null count → docCount under-claims as 0 rather than overclaiming.
        assertEquals(0, skipper.docCount(0));
    }

    public void testDocCountSubtractsNulls() throws Exception {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0 },
            new long[] { 0 },
            new int[] { 0 },
            new long[] { 30 },
            new long[] { 1 },
            new long[] { 9 },
            100
        );
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(idx, 100);
        skipper.advance(0);
        assertEquals(70, skipper.docCount(0));
        assertEquals(70, skipper.docCount());
    }
}
