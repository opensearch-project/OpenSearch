/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

import static org.opensearch.common.util.PageCacheRecycler.LONG_PAGE_SIZE;

/**
 * Double array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 *
 * @opensearch.internal
 */
final class BigDoubleArray extends AbstractBigArray implements DoubleArray {

    private static final BigDoubleArray ESTIMATOR = new BigDoubleArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    private long[][] pages;

    /** Constructor. */
    BigDoubleArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(LONG_PAGE_SIZE, bigArrays, clearOnResize);
        this.size = size;
        pages = new long[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newLongPage(i);
        }
    }

    @Override
    public double get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return Double.longBitsToDouble(pages[pageIndex][indexInPage]);
    }

    @Override
    public double set(long index, double value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final long[] page = pages[pageIndex];
        final double ret = Double.longBitsToDouble(page[indexInPage]);
        page[indexInPage] = Double.doubleToRawLongBits(value);
        return ret;
    }

    @Override
    public double increment(long index, double inc) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final long[] page = pages[pageIndex];
        return page[indexInPage] = Double.doubleToRawLongBits(Double.longBitsToDouble(page[indexInPage]) + inc);
    }

    @Override
    protected int numBytesPerElement() {
        return Integer.BYTES;
    }

    /** Change the size of this array. Content between indexes <code>0</code> and <code>min(size(), newSize)</code> will be preserved. */
    @Override
    public void resize(long newSize) {
        final int numPages = numPages(newSize);
        if (numPages > pages.length) {
            pages = Arrays.copyOf(pages, ArrayUtil.oversize(numPages, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
        }
        for (int i = numPages - 1; i >= 0 && pages[i] == null; --i) {
            pages[i] = newLongPage(i);
        }
        for (int i = numPages; i < pages.length && pages[i] != null; ++i) {
            pages[i] = null;
            releasePage(i);
        }
        this.size = newSize;
    }

    @Override
    public void fill(long fromIndex, long toIndex, double value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
        }
        final long longBits = Double.doubleToRawLongBits(value);
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        if (fromPage == toPage) {
            Arrays.fill(pages[fromPage], indexInPage(fromIndex), indexInPage(toIndex - 1) + 1, longBits);
        } else {
            Arrays.fill(pages[fromPage], indexInPage(fromIndex), pages[fromPage].length, longBits);
            for (int i = fromPage + 1; i < toPage; ++i) {
                Arrays.fill(pages[i], longBits);
            }
            Arrays.fill(pages[toPage], 0, indexInPage(toIndex - 1) + 1, longBits);
        }
    }

    /** Estimates the number of bytes that would be consumed by an array of the given size. */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

}
