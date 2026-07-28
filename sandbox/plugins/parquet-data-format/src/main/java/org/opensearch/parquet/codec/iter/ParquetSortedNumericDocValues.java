/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.parquet.bridge.ParquetColumnReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * {@link SortedNumericDocValues} over a multi-valued Parquet primitive column.
 *
 * <p>Repeated columns are not page-cacheable (the page decoder targets single-valued
 * columns), so each {@link #advanceExact(int)} reads the row's repeated values via the
 * column reader's slow path, then sorts them ascending to satisfy Lucene's contract. The
 * per-doc values are buffered in a reused array (Layer 5) and walked by {@link #nextValue()}.
 */
public final class ParquetSortedNumericDocValues extends SortedNumericDocValues {

    private final ParquetColumnReader reader;
    private final int maxDoc;

    private int doc = -1;
    private long[] values = new long[0];
    private int count;
    private int cursor;

    public ParquetSortedNumericDocValues(ParquetColumnReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            count = 0;
            cursor = 0;
            return false;
        }
        doc = target;
        ParquetColumnReader.RepeatedValues rv = reader.readRepeatedAtRow(target);
        count = rv.count();
        cursor = 0;
        if (count == 0) {
            return false; // empty list = missing.
        }
        if (values.length < count) {
            values = new long[count];
        }
        System.arraycopy(rv.bits(), 0, values, 0, count);
        // Lucene's SortedNumeric contract requires ascending order.
        Arrays.sort(values, 0, count);
        return true;
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public long nextValue() {
        return values[cursor++];
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                doc = d;
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
