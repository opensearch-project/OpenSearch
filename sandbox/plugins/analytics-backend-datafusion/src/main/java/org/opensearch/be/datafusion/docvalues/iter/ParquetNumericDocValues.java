/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.index.NumericDocValues;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBatch;
import org.opensearch.be.datafusion.docvalues.bridge.NumericValueReader;

import java.io.IOException;

/**
 * {@link NumericDocValues} over a single-valued Parquet primitive column.
 *
 * <p>Hot path: a presence bit-test plus an in-place read from the reader's resident
 * {@link DecodedBatch}. When the requested document falls outside that batch,
 * {@link NumericValueReader#loadBatchContaining} decodes the batch that holds it (the only step that
 * crosses the native boundary). Float and double values are decoded as their raw IEEE-754 bits, so
 * {@link #longValue()} returns the Lucene-encoded form directly.
 */
public final class ParquetNumericDocValues extends NumericDocValues {

    private final NumericValueReader reader;
    private final int maxDoc;

    private int doc = -1;
    private long currentValue;

    /** Creates a view over {@code reader}'s single-valued numeric column, bounded by {@code maxDoc}. */
    public ParquetNumericDocValues(NumericValueReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            return false;
        }
        doc = target;
        DecodedBatch batch = reader.decodedBatch();
        if (batch == null || batch.contains(target) == false) {
            reader.loadBatchContaining(target);
            batch = reader.decodedBatch();
        }
        boolean present = batch.isPresent(target);
        currentValue = present ? batch.valueAt(target) : 0L;
        return present;
    }

    @Override
    public long longValue() {
        return currentValue;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        if (doc == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
        }
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        int d = target;
        while (d < maxDoc) {
            DecodedBatch batch = reader.decodedBatch();
            if (batch == null || batch.contains(d) == false) {
                reader.loadBatchContaining(d);
                batch = reader.decodedBatch();
            }
            // Dense batches answer immediately; sparse batches skip whole all-null bitmap bytes.
            long next = batch.nextPresentRow(d);
            if (next >= 0) {
                doc = (int) next;
                currentValue = batch.valueAt(next);
                return doc;
            }
            d = (int) batch.lastRow() + 1;
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
