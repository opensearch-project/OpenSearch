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

/**
 * {@link DocValuesSkipper} backed by the Parquet ColumnIndex (per-page min/max/null-count),
 * exposed through the already-loaded {@link ColumnPageIndex}.
 *
 * <p>This is the query-level complement of the codec's internal skips: Layer 3/4 make a
 * requested page cheap to reach, while this skipper lets Lucene's range machinery avoid
 * requesting excluded pages at all — zero decode, zero FFM, zero iteration for any page whose
 * [min, max] does not intersect the query range.
 *
 * <p>Single level: level 0 intervals are Parquet pages (~20k rows). The Row ID = Doc ID
 * invariant makes page row ranges directly usable as doc ID ranges. Pages with unknown
 * min/max carry the sentinel ({@code Long.MIN_VALUE}, {@code Long.MAX_VALUE}) from the native
 * page-index load, so they intersect every query range and are never wrongly skipped.
 *
 * <p>Only served for integer-shaped columns (long/int/date/boolean): their raw-bits value
 * order matches numeric order. Float/double doc values are raw IEEE-754 bits whose order
 * diverges for negatives, so the producer declines to build a skipper for them (see
 * {@link ParquetDocValuesProducer#getSkipper}).
 */
final class ParquetDocValuesSkipper extends DocValuesSkipper {

    private final ColumnPageIndex pageIndex;
    private final int maxDoc;
    private final long globalMin;
    private final long globalMax;
    private final int globalDocCount;

    /** Current page index, -1 before the first advance, pageCount when exhausted. */
    private int page = -1;

    ParquetDocValuesSkipper(ColumnPageIndex pageIndex, int maxDoc) {
        this.pageIndex = pageIndex;
        this.maxDoc = maxDoc;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long withValue = 0;
        for (int p = 0; p < pageIndex.pageCount(); p++) {
            min = Math.min(min, pageIndex.minOf(p));
            max = Math.max(max, pageIndex.maxOf(p));
            withValue += pageDocCount(pageIndex, p);
        }
        this.globalMin = pageIndex.pageCount() == 0 ? Long.MIN_VALUE : min;
        this.globalMax = pageIndex.pageCount() == 0 ? Long.MAX_VALUE : max;
        this.globalDocCount = (int) withValue;
    }

    /**
     * Documents with a value in page {@code p}. When the page's null count is unknown (-1) this
     * UNDER-claims (0): consumers use docCount for density checks (all-docs-have-values fast
     * paths), where overclaiming would produce wrong results and underclaiming merely disables
     * an optimization.
     */
    private static long pageDocCount(ColumnPageIndex pageIndex, int p) {
        long nulls = pageIndex.nullCountOf(p);
        return nulls < 0 ? 0 : pageIndex.numRowsOf(p) - nulls;
    }

    @Override
    public void advance(int target) {
        if (target >= maxDoc) {
            page = pageIndex.pageCount();
        } else {
            page = pageIndex.pageForRow(target);
        }
    }

    private boolean exhausted() {
        return page >= pageIndex.pageCount();
    }

    @Override
    public int numLevels() {
        return 1;
    }

    @Override
    public int minDocID(int level) {
        if (page < 0) {
            return -1;
        }
        return exhausted() ? DocIdSetIterator.NO_MORE_DOCS : (int) pageIndex.firstRowOf(page);
    }

    @Override
    public int maxDocID(int level) {
        if (page < 0) {
            return -1;
        }
        return exhausted() ? DocIdSetIterator.NO_MORE_DOCS : (int) (pageIndex.firstRowOf(page) + pageIndex.numRowsOf(page) - 1);
    }

    @Override
    public long minValue(int level) {
        return pageIndex.minOf(page);
    }

    @Override
    public long maxValue(int level) {
        return pageIndex.maxOf(page);
    }

    @Override
    public int docCount(int level) {
        return (int) pageDocCount(pageIndex, page);
    }

    @Override
    public long minValue() {
        return globalMin;
    }

    @Override
    public long maxValue() {
        return globalMax;
    }

    @Override
    public int docCount() {
        return globalDocCount;
    }
}
