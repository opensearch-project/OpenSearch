/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.codec.OrdinalTable;

import java.io.IOException;

/**
 * {@link SortedDocValues} backed by a per-segment {@link OrdinalTable} for a single-valued
 * Parquet keyword/ip column. The ordinal table is built once (lazily) by the producer; this
 * iterator only walks the per-row ordinal array and serves {@code lookupOrd} from the sorted
 * term dictionary.
 */
public final class ParquetSortedDocValues extends SortedDocValues {

    private final OrdinalTable table;
    private final int maxDoc;

    private int doc = -1;
    private int currentOrd = -1;

    public ParquetSortedDocValues(OrdinalTable table, int maxDoc) {
        this.table = table;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentOrd = -1;
            return false;
        }
        doc = target;
        currentOrd = table.ordForRow(target);
        return currentOrd != -1;
    }

    @Override
    public int ordValue() {
        return currentOrd;
    }

    @Override
    public BytesRef lookupOrd(int ord) {
        return table.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
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
            if (table.ordForRow(d) != -1) {
                doc = d;
                currentOrd = table.ordForRow(d);
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        currentOrd = -1;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
