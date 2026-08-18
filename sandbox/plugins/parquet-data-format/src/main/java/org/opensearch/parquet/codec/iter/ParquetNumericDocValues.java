/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.NumericDocValues;
import org.opensearch.parquet.codec.bridge.DecodedBatch;
import org.opensearch.parquet.codec.bridge.NumericValueReader;

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
    private boolean currentPresent;

    public ParquetNumericDocValues(NumericValueReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentPresent = false;
            return false;
        }
        doc = target;
        DecodedBatch batch = reader.decodedBatch();
        if (batch == null || batch.contains(target) == false) {
            reader.loadBatchContaining(target);
            batch = reader.decodedBatch();
        }
        currentPresent = batch.isPresent(target);
        currentValue = currentPresent ? batch.valueAt(target) : 0L;
        return currentPresent;
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
