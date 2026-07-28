/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.codec.OrdinalTable;

import java.io.IOException;

/**
 * {@link SortedSetDocValues} backed by a per-segment {@link OrdinalTable} for a multi-valued
 * Parquet keyword/ip column.
 *
 * <p>The ordinal table's CSR layout stores each row's distinct ordinals in ascending order.
 * Following Lucene 10's contract, {@link #advanceExact(int)} returns {@code false} on an empty
 * list, {@link #docValueCount()} reports the row's ordinal count, and {@link #nextOrd()} is
 * called exactly {@code docValueCount()} times, returning strictly ascending ordinals.
 */
public final class ParquetSortedSetDocValues extends SortedSetDocValues {

    private final OrdinalTable table;
    private final int maxDoc;

    private int doc = -1;
    private int count;
    private int cursor;

    public ParquetSortedSetDocValues(OrdinalTable table, int maxDoc) {
        this.table = table;
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
        count = table.countForRow(target);
        cursor = 0;
        return count > 0;
    }

    @Override
    public long nextOrd() {
        return table.ordForRow(doc, cursor++);
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public BytesRef lookupOrd(long ord) {
        return table.lookupOrd((int) ord);
    }

    @Override
    public long getValueCount() {
        return table.valueCount();
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
            if (table.countForRow(d) > 0) {
                doc = d;
                count = table.countForRow(d);
                cursor = 0;
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        count = 0;
        cursor = 0;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
